package dcp.business.actors

import akka.actor.Actor
import akka.actor.SupervisorStrategy.Stop
import dcp.business.codemap.CodeMap
import dcp.business.service.{UserTracks, CleanMachine}
import dcp.common._
import dcp.config.DynConfiguration
import scala.concurrent.duration._


class DcpActor extends Actor {
  private[DcpActor] case class UpdateCodeMapTrigger()
  private lazy val codemapActor = context.actorSelection("/user/DcpSystemActor/codemapActor")
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val esjRouter = context.actorSelection("/user/DcpSystemActor/esjRouter")
  private lazy val hbaseRouter = context.actorSelection("/user/DcpSystemActor/hbaseRouter")
  private lazy val oryxRouter = context.actorSelection("/user/DcpSystemActor/oryxRouter")
  private lazy val cleanMachine = new CleanMachine()
  private var codemapsOpt: Option[Map[String, CodeMap]] = None
  private lazy val timeinterval = this.timeinterval4updateCodeMap

  private def timeinterval4updateCodeMap = {
    //更新codemap的时间间隔, 默认为12, 单位小时, 要比codemap.timeinterval 小
    DynConfiguration.getIntOrElse("dcp.updatecodemap.timeinterval", 24)
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
    * 定时向自己发消息，触发向codemap actor拿codemap数据
    */
  override def preStart() = {
    import context.dispatcher
    context.system.scheduler.schedule(10.seconds, this.timeinterval.hours, self, UpdateCodeMapTrigger())
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} stared")
  }

  private def cleanUserTracks(codemap: Map[String, CodeMap], userTrackStr: String): Option[UserTracks] = {
    try {
      //做异常处理, 免得清洗的时候出现一些不可预料的情况而使程序死亡
      cleanMachine.clean(userTrackStr, codemap)
    } catch {
      case ex: Exception => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path}, ${ex.getStackTraceString}")
        None
    }
  }

  private def broadcastUserTracks(userTracksOpt: Option[UserTracks]) = {
    userTracksOpt match {
      case None =>
      case Some(userTracks) =>
        val cleanedTrack = CleanedTrack(userTracks)
        esjRouter ! cleanedTrack
        oryxRouter ! cleanedTrack
        hbaseRouter ! cleanedTrack
    }
  }

  private def stopDcpActor() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  def receive = {
    case WebTrack(webTrackStr) =>
      codemapsOpt match {
        case None => //do nothing
        case Some(codemap) =>
          val userTracksOpt = this.cleanUserTracks(codemap, webTrackStr)
          this.broadcastUserTracks(userTracksOpt)
      }

    case UpdateCodeMapTrigger() => codemapActor ! UpdateCodeMap() //触发定时更新
    case CodeMapData(codemapsdata) => codemapsOpt = Some(codemapsdata)
    case Stop => this.stopDcpActor()
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}