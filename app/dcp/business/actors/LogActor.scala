package dcp.business.actors

import BoDaoCommon.Log.LogTool
import akka.actor.Actor
import akka.actor.SupervisorStrategy.Stop
import dcp.common._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import dcp.config.DynConfiguration

case class FlushLog()

class LogActor extends Actor {
  private lazy val logs = ArrayBuffer[LogMsg]()
  /**
    * 为了练习使用偏函数
    * 直接使用filter更好
    */
  private lazy val errlogs: PartialFunction[LogMsg, String] = {
    case logMsg if logMsg.lever == LogLevelEnum.Err => logMsg.log
  }
  private lazy val warnlogs: PartialFunction[LogMsg, String] = {
    case logMsg if logMsg.lever == LogLevelEnum.Warn => logMsg.log
  }
  private lazy val infologs: PartialFunction[LogMsg, String] = {
    case logMsg if logMsg.lever == LogLevelEnum.Info => logMsg.log
  }

  private def flushlogs() = {
    LogTool.errorLog(logs.collect(errlogs))
    LogTool.warningLog(logs.collect(warnlogs))
    LogTool.infoLog(logs.collect(infologs))
  }

  /**
    * 定时刷写
    */
  override def preStart() = {
    val jifies = DynConfiguration.getIntOrElse("log.jifies", 30)
    import context.dispatcher
    context.system.scheduler.schedule(3.seconds, jifies.seconds, self, FlushLog())
  }

  /**
    * 提取器
    */
  private[LogActor] object flushLog {
    def unapply(log: LogMsg): Boolean = log.flush
  }

  private[LogActor] object cacheLog {
    def unapply(log: LogMsg): Boolean = !log.flush
  }

  private def flushlog(logMsg: LogMsg) = {
    logMsg.lever match {
      case LogLevelEnum.Info => LogTool.infoLog(logMsg.log)
      case LogLevelEnum.Warn => LogTool.warningLog(logMsg.log)
      case LogLevelEnum.Err => LogTool.errorLog(logMsg.log)
    }
  }

  /**
    * 缓存数据，一次写入
    * @param logMsg 日志数据
    * @return
    */
  private def cachelogs(logMsg: LogMsg) = {
    logs += logMsg
  }

  private def stopLoger() = {
    this.flushlogs()  //将缓存写入
    context.stop(self)
  }

  def receive = {
    case log @ flushLog() => flushlog(log)  //为了练习提取器，使用守卫更好

    case log @ cacheLog() => cachelogs(log)

    case DeleteLog(lever, date) =>
      lever match {
        case LogLevelEnum.Info => LogTool.deleteInfoLog(date)
        case LogLevelEnum.Warn => LogTool.deleteWarningLog(date)
        case LogLevelEnum.Err => LogTool.deleteErrorLog(date)
      }

    case DeleteDayLogs(date) => LogTool.deleteDayLog(date)

    case FlushLog() =>
      this.flushlogs()
      if (logs.nonEmpty) logs.clear()  //清除缓存

    case Stop => this.stopLoger()
  }

}