/**
 * Copyright (c) 2016, BoDao, Inc. All Rights Reserved.
 *
 * Author@ cwx
 *
 * 本程序实现 esj actor 功能，接收完处理后的数据之后把这些数据上传到Fqueue中，以便
 * 之后让ESJ模块使用。esj actor采用有限状态机，状态有EsjStart：初始状态，EsjRun：
 * 运行状态和EsjFull：buffer到达最大存储量的状态。当缓存数据达到buffer最大值时或
 * 接到flush消息时，把buffer的数据发送给buffer到达最大存储量。
 */

package dcp.business.actors

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._
import dcp.business.service.TracksFormattingTool
import dcp.common._
import dcp.config.DynConfiguration
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

case class EsjContent(js: JValue) extends FSMContent[JValue](js)
case class EsjBufferCache(bufferCache: ArrayBuffer[JValue]) extends FSMBuffer[JValue](bufferCache)

class EsjRouter extends Actor {
  import akka.routing.RoundRobinPool
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private var esjRouterOpt: Option[ActorRef] = None

  private def esjActorAmount = DynConfiguration.getIntOrElse("actor.esj.amount", 1)

  private def initEsjRouter(execActorAmount: Int, routerName: String = "esjRouter") = {
    val esjEscalator = OneForOneStrategy() {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} ${ex.getStackTraceString}")
        Restart
    }

    val router = context.actorOf(RoundRobinPool(execActorAmount,
      supervisorStrategy = esjEscalator).props(Props[EsjActor]), routerName)

    Some(context.watch(router))
  }

  override def preStart() = {
    this.esjRouterOpt = this.initEsjRouter(this.esjActorAmount)
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  private def routeUserTracks(formattedTracks: JValue, esjRouterOpt: Option[ActorRef]) = {
    esjRouterOpt match {
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} esjRouter never initialized")
      case Some(esjRouter) => esjRouter ! EsjContent(formattedTracks)
    }
  }

  private def stopEsjRouter(esjRouterOpt: Option[ActorRef]) = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  def receive = {
    case CleanedTrack(userTracks) =>
      val formattedTracksOpt = TracksFormattingTool.formattingTrack4esj(userTracks)
      formattedTracksOpt match {
        case None =>
        case Some(formattedTracks) => this.routeUserTracks(formattedTracks, this.esjRouterOpt)
      }

    case Stop => this.stopEsjRouter(this.esjRouterOpt)
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}

class EsjActor extends Actor with FSM[FSMState, FSMData] {
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val fqueueRouterName = "/user/DcpSystemActor/fqueueRouter"
  private lazy val cache = ArrayBuffer[JValue]()
  private lazy val TimeInterval = DynConfiguration.getIntOrElse("esj.TimeInterval", 5) //默认5分钟
  private lazy val maxStorage = this.maxStorageSize
  private lazy val fqName = DynConfiguration.getStringOrElse("fqueue.queuename.esj", "track_BOdao2015*")

  private def maxStorageSize = {
    val size = DynConfiguration.getIntOrElse("esj.maxStorage", 4) //默认为5个
    if (size > 8) 8 else if (size <= 0) 4 else size //限制于fqueue的消息大小
  }

  private def sendBuffer2Fq(fqueueRouterName: String, fqName: String, buffer: ArrayBuffer[JValue]): Boolean = {
    val data = compact(render(buffer.toList))
    val fqueueRouter = context.actorSelection(fqueueRouterName)
    fqueueRouter ! EsjTrack(fqName, data)
    true
  }

  setTimer("EsjTimer", FSMFlush, this.TimeInterval.minutes, repeat = true)

  override def preStart() = {
    self ! FSMInit
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
   * 为初始化的状态
   */
  startWith(FSMStart, FSMUninitialized)

  when(FSMStart) {
    case Event(FSMInit, FSMUninitialized) => stay using EsjBufferCache(cache)

    case Event(EsjContent(js), esjBufferCache @ EsjBufferCache(buffer)) =>
      buffer += js
      goto(FSMRunning) using esjBufferCache.copy(bufferCache = buffer)

    case Event(FSMFlush, esjBufferCache @ EsjBufferCache(buffer)) =>
      stay using EsjBufferCache(buffer)
  }

  when(FSMRunning) {
    case Event(EsjContent(js), esjBufferCache @ EsjBufferCache(buffer)) =>
      buffer += js
      buffer.length match {
        case this.maxStorage => goto(FSMFill) using esjBufferCache.copy(bufferCache = buffer)
        case _ => stay using esjBufferCache.copy(bufferCache = buffer)
      }

    case Event(FSMFlush, esjBufferCache @ EsjBufferCache(buffer)) =>
      this.sendBuffer2Fq(this.fqueueRouterName, this.fqName, buffer)
      buffer.clear()
      goto(FSMStart) using EsjBufferCache(buffer)
  }

  when(FSMFill) {
    case Event(EsjContent(js), esjBufferCache @ EsjBufferCache(buffer)) =>
      this.sendBuffer2Fq(this.fqueueRouterName, this.fqName, buffer)
      buffer.clear()
      buffer += js
      goto(FSMRunning) using EsjBufferCache(buffer)

    case Event(FSMFlush, esjBufferCache @ EsjBufferCache(buffer)) =>
      this.sendBuffer2Fq(this.fqueueRouterName, this.fqName, buffer)
      buffer.clear()
      goto(FSMStart) using EsjBufferCache(buffer)
  }

  initialize()
}