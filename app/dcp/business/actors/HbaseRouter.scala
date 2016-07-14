/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序实现hbase router
 * 将接收到的usertracks转发到各个HbaseMessenger
 * HbaseMessenger是一个状态机类, 保存了FSMStart, FSMRunning, FSMFill这3种状态
 * 当HbaseMessenger的缓存满了或者刷写时间到了的时候, 将数据put到hbase上
 * 当前需要写入的数据表是WebTrackData, 用于保存清洗后的用户轨迹数据
 */

package dcp.business.actors

import scala.collection.mutable.ArrayBuffer
import akka.actor.SupervisorStrategy.{Stop, Restart}
import akka.actor._

import hapi.{HWriter, HbaseTool}
import hapi.Puter.HBFamilyPuter

import dcp.common._
import dcp.business.db.{HbaseStorager, HbaseDB}
import dcp.business.service.{TracksFormattingTool, UserTracks}
import dcp.config.DynConfiguration

case class HbaseBufferCache(bufferCache: ArrayBuffer[UserTracks]) extends FSMBuffer[UserTracks](bufferCache)
case class HbaseContent(userTrack: UserTracks) extends FSMContent[UserTracks](userTrack)

/**
  * hbase的路由类
  * 将接收到的usertracks转发到各个HbaseMessenger
  */
class HbaseRouter extends Actor {
  import akka.routing.RoundRobinPool
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  /* 这种类型的hbase actor 用来管理rawtrackdata 表 */
  private var hbaseWebTrackRouter: Option[ActorRef] = None

  private def hbaseWebTracksActorsAmount = {
    DynConfiguration.getIntOrElse("actor.hbase.webtrack.amount", 1)
  }

  private def hbaseClientExecutorPoolSwitch = {
    DynConfiguration.getBooleanOrElse("hbase.client.executor.switch", default = false)
  }

  private def hbaseClientExecutorPoolSize= {
    DynConfiguration.getIntOrElse("hbase.client.executor.amount", 2)
  }

  private def initHbaseRouter(execActorAmount: Int, routerName: String = "hbaseRouter") = {
    val hbaseEscalator = OneForOneStrategy() {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} ${ex.getStackTraceString}")
        Restart
    }

    val router = context.actorOf(RoundRobinPool(execActorAmount,
      supervisorStrategy = hbaseEscalator).props(Props[HbaseMessenger]), routerName)

    Some(context.watch(router))
  }

  private def clusterConfigFileName = {
    DynConfiguration.getStringOrElse("hbase.systemconfig.file", "./DynConfig/HCluster.conf")
  }

  /**
    * 初始化hbase的连接, 如果需要连接池来处理数据,则开启线程池
    */
  private def initHbaseConnection() = {
    try {
      if (this.hbaseClientExecutorPoolSwitch) {
        HbaseTool.apply(this.clusterConfigFileName, this.hbaseClientExecutorPoolSize)
      } else {
        HbaseTool.apply(this.clusterConfigFileName)
      }
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} ${ex.getStackTraceString}")
    }
  }

  override def postStop() = {
    HbaseTool.closeConnect()
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /* 初始化状态机 */
  override def preStart() = {
    this.initHbaseConnection()  //没有捕获可能发生的异常, 父actor需要监控这个特殊的异常
    HbaseDB.createHbaseDB(HbaseTool.getAdmin)
    this.hbaseWebTrackRouter = this.initHbaseRouter(this.hbaseWebTracksActorsAmount)
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} stared")
  }

  private def routeUserTracks(userTracks: UserTracks, hbaseWebTrackRouter: Option[ActorRef]) = {
    hbaseWebTrackRouter match {
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} hbaseWebTrackRouter never initialized")
      case Some(router) => router ! HbaseContent(userTracks)
    }
  }

  private def stopHbaseRouter(hbaseWebTrackRouterOpt: Option[ActorRef]) = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  def receive = {
    case CleanedTrack(userTracks) =>
      this.routeUserTracks(userTracks, this.hbaseWebTrackRouter)
    case Stop => this.stopHbaseRouter(this.hbaseWebTrackRouter)
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}

/**
  * 定义了 HbaseMessenger 的模板
  * 这是一个有限状态机, 只有3种状态分别为: FSMStart, FSMRunning, FSMFill
  *
  * FSMStart: 这个状态是初始化的状态, 如果收到usertracks消息, 将转到FSMRunning状态.
  * 如果这个时候收到flush消息, 则会保存现状
  *
  * FSMRunning: 这个状态是介于开始和满的状态的中间态,
  * 这时如果接到usertracks消息, 如果加入这个消息后队列满了,则转换到FSMFill
  * 如果接到flush消息则会将缓存里的数据put到hbase上, 回到FSMStart
  *
  * FSMFill: 这个状态是缓存满的时候.
  * 这时如果接收到usertracks, 会将消息加入到缓存,然后将缓存的内容put到hbase上
  * 如果接收到flush消息, 会将缓存的内容put到hbase上
  * 回到FSMStart
  */
trait HbaseFSM extends Actor with FSM[FSMState, FSMData] {
  protected lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val cache = ArrayBuffer[UserTracks]()
  protected lazy val maxCacheSize = this.maxStorageSize

  private def maxStorageSize = {
    /* 缓存队列的大小, 默认为2w */
    DynConfiguration.getIntOrElse("hbase.webtrackdata.storage.maxsize", 20000)
  }

  /**
    * 为初始化的状态
    */
  startWith(FSMStart, FSMUninitialized)

  protected def flushHbaseData(hbaseData: HbaseBufferCache): Boolean = false

  when(FSMStart) {
    case Event(FSMInit, FSMUninitialized) =>
      logActor ! LogMsg(LogLevelEnum.Info, s"FSM Actor ${self.path} init finished")
      stay using HbaseBufferCache(this.cache)

    case Event(HbaseContent(userTrack), hbaseBufferCache @ HbaseBufferCache(bufferCache)) =>
      bufferCache += userTrack
      goto(FSMRunning) using hbaseBufferCache.copy(bufferCache = bufferCache)

    case Event(FSMFlush, hbaseBufferCache @ HbaseBufferCache(bufferCache)) =>
      stay using hbaseBufferCache
  }

  when(FSMRunning) {
    case Event(HbaseContent(userTrack), hbaseBufferCache @ HbaseBufferCache(bufferCache)) =>
      bufferCache += userTrack
      bufferCache.length match {
        case this.maxCacheSize => goto(FSMFill) using hbaseBufferCache.copy(bufferCache = bufferCache)//goto fill
        case _ => stay using hbaseBufferCache.copy(bufferCache = bufferCache)
      }

    case Event(FSMFlush, hbaseBufferCache @ HbaseBufferCache(bufferCache)) =>
      this.flushHbaseData(hbaseBufferCache)
      hbaseBufferCache.bufferCache.clear()
      goto(FSMStart) using hbaseBufferCache.copy(bufferCache = bufferCache)
  }

  when(FSMFill) {
    case Event(HbaseContent(userTrack), hbaseBufferCache @ HbaseBufferCache(bufferCache)) =>
      bufferCache += userTrack
      this.flushHbaseData(hbaseBufferCache)
      hbaseBufferCache.bufferCache.clear()
      goto(FSMStart) using hbaseBufferCache.copy(bufferCache = bufferCache)

    case Event(FSMFlush, hbaseBufferCache @ HbaseBufferCache(bufferCache)) =>
      this.flushHbaseData(hbaseBufferCache)
      hbaseBufferCache.bufferCache.clear()
      goto(FSMStart) using hbaseBufferCache.copy(bufferCache = bufferCache)
  }

  initialize()
}

/**
  * 定时刷写数据,将数据put到hbase上
  * 这个类是管理 WebTrackData 数据表的
  */
class HbaseMessenger extends HbaseFSM {
  import org.apache.hadoop.hbase.client.Durability
  import scala.concurrent.duration._
  import scala.language.implicitConversions

  private lazy val tableName = this.hbaseTableName
  private lazy val familyName = "Tracks"
  private lazy val hbWriter = new HWriter()
  private lazy val hbaseBufferSize = this.hbaseClientBufferSize
  private lazy val durability = this.hbaseClientDurability
  private lazy val ttl = this.hbaseClientTTl

  private def webtrackDBName = DynConfiguration.getStringOrElse("hbase.table.webtrack", "WebTrackData")
  private def businessName = DynConfiguration.getStringOrElse("business.name", "youmaiba")
  private def hbaseTableName = s"${this.businessName}_${this.webtrackDBName}"
  private def timeInterval = DynConfiguration.getIntOrElse("hbase.webtrackdata.TimeInterval", 5)

  /**
    * 默认的ttl为一年
    */
  private def hbaseClientTTl = DynConfiguration.getLongOrElse("hbase.client.ttl", 31536000)

  private implicit def resolveHbaseClientDurability(level: String): Durability = {
    level match {
      case "ASYNC_WAL" => Durability.ASYNC_WAL
      case "SYNC_WAL" => Durability.SYNC_WAL
      case "FSYNC_WAL" => Durability.FSYNC_WAL
      case "SKIP_WAL" => Durability.SKIP_WAL
      case "USE_DEFAULT" => Durability.USE_DEFAULT
      case _ => Durability.USE_DEFAULT
    }
  }

  private def hbaseClientDurability: Durability = {
    val durabilityStr = DynConfiguration.getStringOrElse("hbase.wal.level", "USE_DEFAULT")
    durabilityStr
  }

  private def hbaseClientBufferSize = { //默认为3M
    val megabytes = DynConfiguration.getIntOrElse("hbase.client.buffersize", 3)
    if (megabytes <= 0) 1024 * 1024 * 3
    else megabytes * 1024 * 1024
  }

  private def setHbasePuter(hfp: HBFamilyPuter) = {
    hfp.durability = this.durability
    hfp.ttl = this.ttl
    hfp.bufSize = this.hbaseBufferSize
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
    * 父actor负责启动和初始化hbasetool object
    * 初始化自己的内部状态
    */
  override def preStart() = {
    self ! FSMInit
  }

  /* 设置刷写时间间隔, 默认为300秒 */
  setTimer("HbaseClock", FSMFlush, this.timeInterval.minutes, repeat = true)

  override def flushHbaseData(hbaseData: HbaseBufferCache): Boolean = {
    val trackInHbaseOpts = hbaseData.bufferCache map { elem =>
      TracksFormattingTool.formattingTrack4Hbase(elem)
    }

    val puter = new HBFamilyPuter(HbaseTool.getHbaseConnection, this.tableName, this.familyName)
    this.setHbasePuter(puter)

    val status = HbaseStorager.putTracks2Hbase(trackInHbaseOpts, puter, hbWriter)
    if (!status) logActor ! LogMsg(LogLevelEnum.Err, s"FSM Actor ${self.path} saved tracks fail")
    status
  }
}