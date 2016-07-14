/*
 * Copyright (c) 2016, BoDao, Inc. All Rights Reserved.
 *
 * Author@ cwx
 *
 * 本程序实现statistics actor, 功能时接收含有访客浏览内容的信息和
 * 浏览流量内容的信息，进行访客量的统计和浏览流量的统计。以每个小
 * 时为单位把统计内容导入文件中，每天创建一个新的文件。
 */

package dcp.business.actors

import scala.concurrent.duration._
import scala.collection.mutable.{Map => muMap}

import akka.actor.SupervisorStrategy.Stop
import BoDaoCommon.Date.DateTool
import BoDaoCommon.File.LocalFileTool
import akka.actor.{ActorRef, Props, Actor}
import dcp.config.{DynConfiguration, ConfigurationEngine}
import dcp.common.{StatisticsMsg, LogLevelEnum, LogMsg}

case class VisitorsStatistics(inc: Long)
case class TrafficStatistics(inc: Long)
case class RecordsStatistics()

class StatisticsActor extends Actor {
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private var visitorsStatisticsActorOpt: Option[ActorRef] = None
  private var trafficStatisticsActorOpt: Option[ActorRef] = None

  override def preStart() = {
    val visitorsStatisticsActor = context.actorOf(Props[VisitorsStatisticsActor], "visitorsStatisticsActor")
    val trafficStatisticsActor = context.actorOf(Props[TrafficStatisticsActor], "trafficStatisticsActor")
    this.visitorsStatisticsActorOpt = Some(context.watch(visitorsStatisticsActor))
    this.trafficStatisticsActorOpt = Some(context.watch(trafficStatisticsActor))
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  private def stopStatisticsActor() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  def receive = {
    case StatisticsMsg(visitors, traffic) =>
      this.visitorsStatisticsActorOpt match {
        case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} itemActor never initialized")
        case Some(visitorsStatisticsActor) => visitorsStatisticsActor ! VisitorsStatistics(visitors)
      }

      this.trafficStatisticsActorOpt match {
        case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} itemActor never initialized")
        case Some(trafficStatisticsActor) => trafficStatisticsActor ! TrafficStatistics(traffic)
      }

    case Stop => this.stopStatisticsActor()
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}

trait StatisticsTrait extends Actor {
  protected val name: String
  private var buffer = muMap[String, String]()
  private val statisticsRoot = this.statisticsRootDir
  private val cacheRoot = this.cacheRootDir

  protected def initBuffer: Boolean = {
    for (i <- 0 to 23) {
      if (i < 10) this.buffer += (s"0$i" -> "0")
      else this.buffer += (s"$i" -> "0")
    }
    true
  }

  private def cacheRootDir = {
    ConfigurationEngine.getDir("cache.dir", "./data")
  }

  private def statisticsRootDir = {
    ConfigurationEngine.getDir("cache.statistics.dir", "statistics")
  }

  private def getStatisticsFileName = s"${this.cacheRoot}/${this.statisticsRoot}/${this.name}-${DateTool.currentDate2Day}"

  private def getStatisticsHour: String = {
    val date = DateTool.currentDate2Hour
    val fields = date.split(" ", 2)
    val hour = fields.last
    hour
  }

  private def addAndSave(buffer1: muMap[String, String], buffer2: Map[String, String], statisticsFileName: String): Boolean = {
    for (i <- 0 to 23) {
      if (i < 10) buffer1(s"0$i") = (buffer1(s"0$i").toLong + buffer2(s"0$i").toLong).toString
      else buffer1(s"$i") = (buffer1(s"$i").toLong + buffer2(s"$i").toLong).toString
    }

    LocalFileTool.save2kv(statisticsFileName, buffer1.toMap, "\t")
    true
  }

  protected def statistics(inc: Long) = {
    val hour = this.getStatisticsHour
    this.buffer(hour) = (this.buffer(hour).toLong + inc).toString
  }

  protected def records: Boolean = {
    val statisticsFileName = this.getStatisticsFileName
    LocalFileTool.readFile2kv(statisticsFileName, "\t") match {
      case Some(data) =>
        this.addAndSave(this.buffer, data, statisticsFileName)
        initBuffer

      case None =>
        LocalFileTool.save2kv(statisticsFileName, this.buffer.toMap, "\t")
        initBuffer
    }
    true
  }
}

class VisitorsStatisticsActor extends StatisticsTrait {
  private val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  override val name = DynConfiguration.getStringOrElse("statistics.visitors.name", "")

  override def preStart() = {
    val timeinterval = DynConfiguration.getIntOrElse("statistics.visitors.timeinterval", 5)
    val timeWait = DynConfiguration.getIntOrElse("statistics.visitors.timeWait", 10)
    initBuffer

    import context.dispatcher
    context.system.scheduler.schedule(timeWait.seconds, timeinterval.minutes, self, RecordsStatistics())
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  def receive = {
    case VisitorsStatistics(inc) => statistics(inc)
    case RecordsStatistics() => records
  }
}

class TrafficStatisticsActor extends StatisticsTrait {
  private val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  override val name = DynConfiguration.getStringOrElse("statistics.traffic.name", "")

  override def preStart() = {
    val timeinterval = DynConfiguration.getIntOrElse("statistics.traffic.timeinterval", 5)
    val timeWait = DynConfiguration.getIntOrElse("statistics.traffic.timeWait", 10)
    initBuffer

    import context.dispatcher
    context.system.scheduler.schedule(timeWait.seconds, timeinterval.minutes, self, RecordsStatistics())
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  def receive = {
    case TrafficStatistics(inc) => statistics(inc)
    case RecordsStatistics() => records
  }
}