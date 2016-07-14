/**
 * Copyright (c) 2016, BoDao, Inc. All Rights Reserved.
 *
 * Author@ cwx
 *
 * 本程序实现 hdfs actor，定时向远程程序发送原数据本地文件路径和
 * hdfs的文件路径，让远程程序来把本地文件数据上传到hdfs上
 */

package dcp.business.actors

import scala.concurrent.duration._

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import akka.actor.Actor
import akka.actor.SupervisorStrategy.Stop

import BoDaoCommon.Date.DateTool
import BoDaoCommon.File.LocalFileTool
import dcp.common.{LogLevelEnum, LogMsg}
import dcp.config.{ConfigurationEngine, DynConfiguration}

class PutFileToHdfsException(message: String) extends Exception(message)
case class SendFileNames()
case class StateMassage(state: String, massage: String)

class HdfsActor extends Actor {
  private lazy val remoteActorPath = DynConfiguration.getStringOrElse("hdfs.remoteActorPath", "")
  private lazy val remoteActor = context.actorSelection(remoteActorPath)
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val busness = DynConfiguration.getStringOrElse("business.name", "youmaiba")
  private lazy val localCacheRoot = this.localRootDir
  private lazy val hdfsCacheRoot = this.hdfsRootDir
  private lazy val cleanedRoot = this.cleanedRootDir
  private [HdfsActor] case class PutFilesToHdfsTrigger() // 定时器

  override def preStart() = {
    val timeinterval = DynConfiguration.getIntOrElse("hdfs.timeinterval", 24)
    val delaytime = DynConfiguration.getIntOrElse("hdfs.delaytime", 20)

    import context.dispatcher
    context.system.scheduler.schedule(10.seconds, timeinterval.hours, self, SendFileNames())
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  private def localRootDir = {
    val path = BoDaoCommon.File.LocalFileTool.pwd()
    val dir = ConfigurationEngine.getDir("cache.dir.absolute", "./data")
    val rootDir = path + "/" +dir

    rootDir
  }

  private def hdfsRootDir = {
    ConfigurationEngine.getDir("hdfs.Hcache.dir", "/skynet")
  }

  private def cleanedRootDir = {
    ConfigurationEngine.getDir("cache.cleaned.dir", "cleaned")
  }

  private def getLocalDir: String = {
    val date = DateTool.daysAgoDate2Day(1)
    val localDir = s"${this.localCacheRoot}/${this.cleanedRoot}/$date/" //本地文件的路径
    localDir
  }

  private def getHdfsDir: String = {
    val date = DateTool.daysAgoDate2Day(1)
    var hdfsDir = s"${this.hdfsCacheRoot}/${this.cleanedRoot}/${this.busness}/"
    val fields = date.split("-", 3)
    for (f <- fields) hdfsDir += (f + "/")
    hdfsDir
  }

  private def sendFileNames() = {
    implicit val formats = DefaultFormats   // Brings in default date formats etc.
    LocalFileTool.getRegFilesNameAtDir(this.getLocalDir) match {
      case Some(files) =>
        files.foreach(file =>{
          val hdfsFile = this.getHdfsDir + file
          val localFile = this.getLocalDir + file
          var fileNames = Map[String, String]()
          fileNames += ("local" -> localFile)
          fileNames += ("hdfs" -> hdfsFile)
          try {
            remoteActor ! compact(render(fileNames))
          } catch {
            case ex: Exception =>
              logActor ! LogMsg(LogLevelEnum.Err, s"at ${self.path} hava err: ${ex.getStackTraceString}")
              val info = s"${self.path} Put local file to hdfs have a err"
              throw new PutFileToHdfsException(info)
          }
        })

      case None => logActor ! LogMsg(LogLevelEnum.Warn, s"${this.getLocalDir} is not found")
    }
  }

  private def resolveFileNames(fileNamesJson: String): Option[StateMassage] = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats
    try {
      val jsObj = parse(fileNamesJson)
      val stateOp = (jsObj \ "status").extractOpt[String]
      val msgOp = (jsObj \ "msg").extractOpt[String]
      for {
        state <- stateOp
        msg <- msgOp
      } yield StateMassage(state, msg)
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} err at resolveFileNames, ${ex.getMessage}")
        None
    }
  }

  private def judgState(stateMassege: StateMassage) = {
    stateMassege.state match {
      case "-1" => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} receice an err statemassege ${stateMassege.massage}")
      case "0" => logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} receice a statemassege ${stateMassege.massage}")
    }
  }

  private def parsingFileNames(fileNamesJson: String) = {
    this.resolveFileNames(fileNamesJson) match {
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} receive statemessage is empty")
      case Some(stateMassge) => this.judgState(stateMassge)
    }
  }

  private def stopHdfsActor: Boolean = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
    true
  }

  def receive = {
    case SendFileNames() => this.sendFileNames()
    case fileNamesJson: String => this.parsingFileNames(fileNamesJson)
    case Stop => this.stopHdfsActor
    case _ => logActor ! LogMsg(LogLevelEnum.Err, "received unknown message")
  }
}