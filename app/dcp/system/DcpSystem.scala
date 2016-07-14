/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序是dcp系统的入口
 * 负责启动整个系统。先检测系统资源，如文件目录、需要的文件、数据库表等，然后初始化系统，生产系统需要的各部分Actor。
 * 然后对需要定时执行的Actor进行调度， 监控整个系统的状态。
 */

package dcp.system

import BoDaoCommon.File.LocalFileTool
import akka.actor.SupervisorStrategy.{Stop, Restart}
import akka.actor._
import dcp.common._
import dcp.config.ConfigurationEngine
import scala.collection.mutable.{Map => MuMap}

class DcpSystem extends Actor {
  private val actorRefs = MuMap[String, ActorRef]()
  private var logActor: ActorRef = null
  private var systemRef: ActorSystem = null

  private def startLogActor(name: String = "logActor") = {
    import dcp.business.actors.LogActor
    logActor = context.watch(context.actorOf(Props[LogActor], name))
    actorRefs += (name -> logActor)
    Thread.sleep(500)
    logActor ! LogMsg(LogLevelEnum.Info, "log actor initialized", flush = true)
  }

  private def createCacheDataDir(): Boolean = {
    val cacheDataDir = ConfigurationEngine.getDir("cache.dir", "./data")
    if (!LocalFileTool.createDir(cacheDataDir)) {
      logActor ! LogMsg(LogLevelEnum.Err, s"created $cacheDataDir fail", flush = true)
      false
    } else {
      true
    }
  }

  private def createStatisticsDir(): Boolean = {
    val statisticsSubDir = ConfigurationEngine.getDir("cache.statistics.dir", "statistics")
    val statisticsDir = s"${ConfigurationEngine.getDir("cache.dir", "./data")}/$statisticsSubDir"
    if (!LocalFileTool.createDir(statisticsDir)) {
      logActor ! LogMsg(LogLevelEnum.Err, s"created $statisticsDir fail", flush = true)
      false
    } else {
      true
    }
  }

  private def createCleanedDataDir(): Boolean = {
    val cleanedSubDir = ConfigurationEngine.getDir("cache.cleaned.dir", "cleaned")
    //父目录不存在，会自动创建
    val cleanedDir = s"${ConfigurationEngine.getDir("cache.dir", "./data")}/$cleanedSubDir/tmp"
    if (!LocalFileTool.createDir(cleanedDir)) {
      logActor ! LogMsg(LogLevelEnum.Err, s"created $cleanedDir fail", flush = true)
      false
    } else {
      true
    }
  }

  private def createCodeMapDir(): Boolean = {
    val codeMapDir = ConfigurationEngine.getDir("codemap.dir", "./DynConfig/codemap")
    if (!LocalFileTool.createDir(codeMapDir)) {
      logActor ! LogMsg(LogLevelEnum.Err, s"created $codeMapDir fail", flush = true)
      false
    } else {
      true
    }
  }


  private def createFiles(): Boolean = {
    if (!createCacheDataDir) return false
    if (!createStatisticsDir()) return false
    if (!createCleanedDataDir()) return false
    if (!createCodeMapDir()) return false
    logActor ! LogMsg(LogLevelEnum.Info, "files created", flush = true)
    true
  }

  /**
    * 这个监控其实用不到, 各个actor都做了异常处理了
    * 但为了扩展, 这部分的异常处理还是做了
    */
  override val supervisorStrategy = OneForOneStrategy() {
    case dcpex: DcpException => Restart
    case hbaseex: HbaseException => Restart
    case hdfsex: HdfsException => Restart
    case esjex: EsjException => Restart
    case fqueueex: FqueueException => Restart
    case codemapex: CodeMapException => Restart
  }

  private def startFqueueActor(name: String = "fqueueRouter") = {
    import dcp.business.actors.FqueueRouter
    val fqActor = context.watch(context.actorOf(Props[FqueueRouter], name))
    actorRefs += (name -> fqActor)
  }

  private def startCodeMapActor(name: String = "codemapActor") = {
    import dcp.business.actors.CodeMapActor
    val codemapActor = context.watch(context.actorOf(Props[CodeMapActor], name))
    actorRefs += (name -> codemapActor)
  }

  private def startDcpRouter(name: String = "dcpRouter") = {
    import dcp.business.actors.DcpRouter
    val dcpRouter = context.watch(context.actorOf(Props[DcpRouter], name))
    actorRefs += (name -> dcpRouter)
  }

  private def startEsjActor(name: String = "esjRouter") = {
    import dcp.business.actors.EsjRouter
    val esjActor = context.watch(context.actorOf(Props[EsjRouter], name))
    actorRefs += (name -> esjActor)
  }

  private def startHbaseActor(name: String = "hbaseRouter") = {
    import dcp.business.actors.HbaseRouter
    val hbaseActor = context.watch(context.actorOf(Props[HbaseRouter], name))
    actorRefs += (name -> hbaseActor)
  }

  private def startOryxActor(name: String = "oryxRouter") = {
    import dcp.business.actors.OryxActor
    val oryxActor = context.watch(context.actorOf(Props[OryxActor], name))
    actorRefs += (name -> oryxActor)
  }

  private def startHdfsActor(name: String = "hdfsActor") = {
    import dcp.business.actors.HdfsActor
    val hdfsActor = context.watch(context.actorOf(Props[HdfsActor], name))
    actorRefs += (name -> hdfsActor)
  }

  private def startStatisticsActor(name: String = "statisticsActor") = {
    import dcp.business.actors.StatisticsActor
    val statisticsActor = context.watch(context.actorOf(Props[StatisticsActor], name))
    actorRefs += (name -> statisticsActor)
  }

  private def startFileCleanActor(name: String = "fileCleanActor") = {
    import dcp.business.actors.FileCleanActor
    val fileCleanActor = context.watch(context.actorOf(Props[FileCleanActor], name))
    actorRefs += (name -> fileCleanActor)
  }

  private def statrtActors() = {
    this.startFqueueActor()
    Thread.sleep(2000)

    this.startCodeMapActor()
    Thread.sleep(2000)

    this.startDcpRouter()
    Thread.sleep(2000)

    this.startEsjActor()
    this.startHbaseActor()
    this.startOryxActor()
    this.startHdfsActor()
    this.startFileCleanActor()  //此 actor 不需要手动关闭
    this.startStatisticsActor() //此 actor 不需要手动关闭
  }

  /**
    * 要按顺序关闭Actor
    */
  private def stopSystem() = {
    val fqueueRouter = context.actorSelection("/user/DcpSystemActor/fqueueRouter")
    fqueueRouter ! Stop

    val codemapActor = context.actorSelection("/user/DcpSystemActor/codemapActor")
    codemapActor ! Stop

    val dcpRouter = context.actorSelection("/user/DcpSystemActor/dcpRouter")
    dcpRouter ! Stop

    val esjRouter = context.actorSelection("/user/DcpSystemActor/esjRouter")
    esjRouter ! Stop

    val hbaseRouter = context.actorSelection("/user/DcpSystemActor/hbaseRouter")
    hbaseRouter ! Stop

    val oryxRouter = context.actorSelection("/user/DcpSystemActor/oryxRouter")
    oryxRouter ! Stop

    val hdfsActor = context.actorSelection("/user/DcpSystemActor/hdfsActor")
    hdfsActor ! Stop

    val fileCleanActor = context.actorSelection("/user/DcpSystemActor/fileCleanActor")
    fileCleanActor ! Stop

    val statisticsActor = context.actorSelection("/user/DcpSystemActor/statisticsActor")
    statisticsActor ! Stop
  }

  private def initDcpSystem(): Boolean = {
    this.startLogActor("logActor")
    if (!createFiles()) return false
    this.statrtActors()
    true
  }

  def receive = {
    case SysStart(system) =>
      this.systemRef = system
      if (!this.initDcpSystem()) {
        logActor ! LogMsg(LogLevelEnum.Err, "data files created fail", flush = true)
        Thread.sleep(1000)  //让logActor写入日志
        system.shutdown()
      }

    case Stop =>
      logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} going to stop system", flush = true)
      this.stopSystem()
      Thread.sleep(8000)  //让所有actor都已经关闭
      logActor ! Stop
      if (systemRef != null) systemRef.shutdown()

    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}