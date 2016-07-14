/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序是一个定时器和路由器
 * 实现定时触发fqueue client去拿原生的web tracks数据。
 * 然后将接收到的数据路由给各个dcp actor
 */

package dcp.business.actors

import scala.concurrent.duration._

import akka.actor.SupervisorStrategy.{Stop, Restart}
import akka.actor.{OneForOneStrategy, Props, ActorRef, Actor}
import akka.routing. RoundRobinPool

import dcp.common._
import dcp.config.DynConfiguration

/**
  * 一个akka路由机制类
  */
class DcpRouter extends Actor {
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val fqRouter = context.actorSelection("/user/DcpSystemActor/fqueueRouter")
  private lazy val statisticsActor = context.actorSelection("/user/DcpSystemActor/statisticsActor")
  private lazy val qName = this.fqName
  private var dcpRouter: Option[ActorRef] = None
  private[DcpRouter] case class GetWebTracksTrigger()

  private def fqName = {
    DynConfiguration.getStringOrElse("fqueue.queuename.rawdata", "rawdata1_BOdao2015*")
  }

  /**
    * 只使用rr算法
    */
  private def initRoute(exeAmount: Int, routerName: String = "dcpSystemRouter"):Option[ActorRef] = {
    val dcpEscalator = OneForOneStrategy() {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} ${ex.getStackTraceString}")
        Restart
    }

    val router = context.actorOf(RoundRobinPool(exeAmount,
      supervisorStrategy = dcpEscalator).props(Props[DcpActor]), routerName)

    Some(context.watch(router))
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
    * 初始化路由
    * 定时给自己发消息，触发从fqueue中拿数据
    */
  override def preStart() = {
    val jifies = DynConfiguration.getIntOrElse("cleaning.jifies", 5) //5分钟
    val delaytime = DynConfiguration.getIntOrElse("cleaning.delaytime", 20) //20秒
    val dcpAmount = DynConfiguration.getIntOrElse("actor.dcp.amount", 1)

    if (dcpAmount > 0) {
      dcpRouter = initRoute(dcpAmount)
    } else {
      dcpRouter = initRoute(1)
    }

    import context.dispatcher
    context.system.scheduler.schedule(delaytime.seconds, jifies.minutes, self, GetWebTracksTrigger())
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} stared")
  }


  private def routeWebTrack(webTrackStr: String, myrouter: Option[ActorRef]) = {
    myrouter match {
      case Some(router) => router ! WebTrack(webTrackStr)
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} sysRouter never initialized")
    }
  }

  private def triggerFqueue(fqName: String) = {
    fqRouter ! FqueueName(fqName)
  }

  private def stopdcpRouter(dcpRouterOpt: Option[ActorRef]) = {
    dcpRouter match {
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"dcpRouter never started, can stop it", flush = true)
      case Some(router) =>
        logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${router.path}", flush = true)
        context.stop(router)
    }
  }

  def receive = {
    case GetWebTracksTrigger() =>
      this.triggerFqueue(this.qName)
    case FqueueData(opstr) =>
      /**
        * 路由webtrack 给dcp actors
        */
      opstr match {
        case Some(webTrackStr) =>
          this.routeWebTrack(webTrackStr, this.dcpRouter)
          statisticsActor ! StatisticsMsg(1, webTrackStr.length)
        case None =>
      }

    case Stop =>
      this.stopdcpRouter(this.dcpRouter)
      context.stop(self)

    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}