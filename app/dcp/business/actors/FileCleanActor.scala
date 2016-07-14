/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序实现遗留文件清理
 * 清理统计文件
 * 清理hdfs 模块上传失败的文件, 先尝试继续上传, 不管成功失败也会删除文件
 */

package dcp.business.actors

import akka.actor.Actor
import akka.actor.SupervisorStrategy.Stop
import dcp.common.{LogLevelEnum, LogMsg}
import dcp.config.{ConfigurationEngine, DynConfiguration}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class FileCleanActor extends Actor {
  private lazy val remoteActorPath = DynConfiguration.getStringOrElse("hdfs.remoteActorPath", "")
  private lazy val remoteActor = context.actorSelection(remoteActorPath)
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val duration = this.cacheDuration
  private lazy val cacheRoot = this.cacheRootDir
  private lazy val localRoot = this.localRootDir
  private lazy val statisticsRoot = this.statisticsRootDir
  private lazy val cleanedRoot = this.cleanedRootDir
  private lazy val hdfsRoot = this.hdfsDir
  private[FileCleanActor] case class CleanTrigger()

  private def cacheDuration = {
    DynConfiguration.getIntOrElse("cache.duration", 8)
  }

  private def date = {
    BoDaoCommon.Date.DateTool.daysAgoDate2Day(this.duration)
  }

  private def hdfsDir = {
    val business = DynConfiguration.getStringOrElse("business.name", "")  //商家名字没有默认值
    val root = ConfigurationEngine.getDir("hdfs.rawdata.dir", "/data/cleaned")
    s"$root/$business"
  }

  private def cacheRootDir = {
    ConfigurationEngine.getDir("cache.dir", "./data")
  }

  private def localRootDir = {
    val path = BoDaoCommon.File.LocalFileTool.pwd()
    val dir = ConfigurationEngine.getDir("cache.dir.absolute", "./data")
    val rootDir = path + dir

    rootDir
  }

  private def statisticsRootDir = {
    ConfigurationEngine.getDir("cache.statistics.dir", "statistics")
  }

  private def cleanedRootDir = {
    ConfigurationEngine.getDir("cache.cleaned.dir", "cleaned")
  }

  /**
    * 清洗完的数据的本地路径
    * @return
    */
  private def dayCleanedDataDir = {
    s"${this.localRoot}/${this.cleanedRoot}/${this.date}"
  }

  /**
    * 统计的根路径
    * @return
    */
  private def statisticsDataDir = {
    s"${this.cacheRoot}/${this.statisticsRoot}"
  }

  private def dayStatisticsVisitorsFileName = {
    s"${this.statisticsDataDir}/visitors-${this.date}"
  }

  private def dayStatisticsTrafficFileName = {
    s"${this.statisticsDataDir}/traffic-${this.date}"
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
    * 定时清理
    */
  override def preStart() = {
    import scala.concurrent.duration._
    val timeinterval = DynConfiguration.getIntOrElse("clean.TimeInterval", 24)
    val delaytime = DynConfiguration.getIntOrElse("clean.delaytime", 20)
    import context.dispatcher
    context.system.scheduler.schedule(delaytime.seconds, timeinterval.hours, self, CleanTrigger)
  }

  private def cleanStatisticsData() = {
    import BoDaoCommon.File.LocalFileTool
    val trafficFileName = this.dayStatisticsTrafficFileName
    val visitorsFileName = this.dayStatisticsVisitorsFileName
    val tStatus = LocalFileTool.deleteRegFile(trafficFileName)
    val vStatus = LocalFileTool.deleteRegFile(visitorsFileName)
    if (!tStatus) logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} clean $trafficFileName fail")
    if (!vStatus) logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} clean $visitorsFileName fail")
  }

  private def hdfsDataDir = {
    val dateFields = BoDaoCommon.Date.DateTool.daysAgoDate2Day(this.cacheDuration).split("-", 3)
    s"${this.hdfsRoot}/${dateFields(0)}/${dateFields(1)}/${dateFields(2)}"
  }

  /**
    * 清理清洗完的数据
    * 这些清洗完的数据, hdfs actor 会在成功put到hdfs后进行删除
    * 如果存在在这里, 就是hdfs actor put 到hdfs失败了
    * 这里再一次尝试, 如果也失败, 直接删除
    */
  private def cleanCleanedData() = {
    import BoDaoCommon.File.LocalFileTool
    val cleanedDataDir = this.dayCleanedDataDir
    val filesOpt = LocalFileTool.getRegFilesNameAtDir(cleanedDataDir)
    val hdfs = this.hdfsDataDir

    filesOpt match {
      case None =>
      case Some(files) =>
        files foreach { fileName =>
          val hdfsFileName = s"$hdfs/$fileName"
          var fileNames = Map[String, String]()
          fileNames += ("local" -> s"$cleanedDataDir/$fileName")
          fileNames += ("hdfs" -> hdfsFileName)
          remoteActor ! compact(render(fileNames))
        }
    }

    LocalFileTool.deleteDir(cleanedDataDir)
  }

  private def stopFileCleanActor() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  def receive = {
    case CleanTrigger =>
      this.cleanStatisticsData()
      this.cleanCleanedData()

    case Stop => this.stopFileCleanActor()
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}