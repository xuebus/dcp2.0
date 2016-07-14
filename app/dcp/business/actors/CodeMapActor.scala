/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序实现codemap actor, 定时向fqueue请求codemap原生json数据
 * 调用CodeMapApp 的接口实现codemap 的解析、持久化、加载到内存。
 * 并且为dcp actors提供更新codemap的接口
 */

package dcp.business.actors

import akka.actor.SupervisorStrategy.Stop

import scala.concurrent.duration._
import akka.actor.Actor
import dcp.common._
import dcp.config.DynConfiguration
import dcp.business.codemap.{CodeMap, CodeMapApp}

class CodeMapActor extends Actor{
  private lazy val qNum = 1   //选择第一条队列获取codemaps，一定存在
  private lazy val qName = this.fqName
  private var codemaps: Option[Map[String, CodeMap]] = null
  private lazy val fqRouter = context.actorSelection("/user/DcpSystemActor/fqueueRouter")
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private[CodeMapActor] case class GetCodeMapsTrigger() //定时器
  private lazy val needCache = this.have2Cache //是否缓存原生数据

  private def fqName = {
    DynConfiguration.getStringOrElse("fqueue.queuename.codemap", "codemap_BOdao2015*")
  }

  private def have2Cache = {
    DynConfiguration.getBooleanOrElse("codemap.cache.rawData", default = true)
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
    * 定时向自己发消息，触发向fqueue拿codemap数据
    * 启动的时候已将将原有的数据加载一遍
    */
  override def preStart() = {
    val timeinterval = DynConfiguration.getIntOrElse("codemap.timeinterval", 24)
    val delaytime = DynConfiguration.getIntOrElse("codemap.delaytime", 20)
    import context.dispatcher
    context.system.scheduler.schedule(delaytime.minutes, timeinterval.hours, self, GetCodeMapsTrigger())
    codemaps = this.loadCodeMaps()
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} stared")
  }

  private def triggerFqueue(obtainNum: Int, fqName: String) = {
    fqRouter ! LastData(fqName) //只获取队列里最后一个数据
  }

  /**
    * 解析json数据
    * 刷写文件，保存新的codemap
    */
  private def persistenceCodeMaps(codemaps: String) = {
    try {
      val parsedCodeMaps = CodeMapApp.parsingCodeMaps(codemaps)
      parsedCodeMaps.keys foreach { name =>
        parsedCodeMaps(name) match {
          case None => logActor ! LogMsg(LogLevelEnum.Warn, s"${self.path}, can't found $name's codemap part")
          case Some(codeMapTuple) =>
            val status = CodeMapApp.persistenceCodeMaps(parsedCodeMaps(name))(CodeMapApp.saveCodeMap)
            if (status) {
              logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} persistence $name's CodeMaps success")
            }else {
              logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} persistence $name's CodeMaps fail")
            }
        }
      }
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} have ex, ${ex.getStackTraceString}")
        //失败时，记录解析失败的codemap json 字符串
        if (needCache) logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} ex codemaps,\n ${ex.getStackTraceString}")
    }
  }

  /**
    * 简单地将codemap加载到内存，备用
    * @return
    */
  private def loadCodeMaps(): Option[Map[String, CodeMap]] = {
    try {
      Some(CodeMapApp.loadCodeMaps())
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} have ex, ${ex.getStackTraceString}")
        None
    }
  }

  private def stopCodeMap() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  def receive = {
    case GetCodeMapsTrigger() => this.triggerFqueue(this.qNum, this.qName)

    case LastFqueueData(opstr) =>
      opstr match {
        case Some(codemapsStr) =>
          //这两个sleep是为了防止队列里有多个codemap,而产生读写codemap文件错误
          Thread.sleep(5000)
          this.persistenceCodeMaps(codemapsStr)
          Thread.sleep(5000)
          //更新codemap
          codemaps = this.loadCodeMaps()
        case None =>
      }

    case UpdateCodeMap() =>
      //dcp actors 定时请求更新
      if (codemaps == null) codemaps = this.loadCodeMaps()
      codemaps match {
        case None => logActor ! LogMsg(LogLevelEnum.Warn, s"${self.path} codemap is none")
        case Some(codemapData) => sender() ! CodeMapData(codemapData)
      }

    case Stop => this.stopCodeMap()

    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}