/**
 * Copyright (c) 2016, BoDao, Inc. All Rights Reserved.
 *
 * Author@ cwx
 *
 * 本程序实现了fqueue actor的功能,其中fqueue send actor的功能是：负责把esj actor发过来
 * 的数据送到存储esj数据的fq队列上。首先由fqueue send路由接收来自esj的数据信息，然后由
 * fqueue send路由分发给fqueue actor。fqueue obtain actor的功能是：当dcp actor和codemap
 * actor发取数据的请求信息给fqueue obtain 路由，由fqueue obtain 路由广播给fqueue obtain
 * actor，再由fqueue obtain actor从对应队列取数据，然后发给fqueue obtain 路由，由路由转发
 * 给dcp actor和codmap actor
 */

package dcp.business.actors

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, ActorSelection}
import akka.routing.{Broadcast, RoundRobinPool}

import dcp.common._
import dcp.config.{ConfigurationEngine, DynConfiguration}
import net.rubyeye.xmemcached.utils.AddrUtil
import net.rubyeye.xmemcached.{MemcachedClient, XMemcachedClientBuilder}

class InitFqueueClientException(message: String) extends Exception(message)
class GetQueueException(message: String) extends Exception(message)
class SendQueueException(message: String) extends Exception(message)

/**
  * 实现两种router是为了分开收和发消息, 实现读写分离
  */
class FqueueRouter extends Actor {
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private var fqObtainRouterOpt: Option[ActorRef] = None
  private var fqSendRouterOpt: Option[ActorRef] = None
  private lazy val dcpRouter = context.actorSelection("/user/DcpSystemActor/dcpRouter")
  private lazy val codemapActor = context.actorSelection("/user/DcpSystemActor/codemapActor")
  private lazy val fqRouterEscalator = OneForOneStrategy() {
    case ex: Exception =>
      logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} ${ex.getStackTraceString}")
      Restart
  }

  private def obtainAmount = DynConfiguration.getIntOrElse("actor.fclient.obtainer.amount", 1)
  private def senderAmount = DynConfiguration.getIntOrElse("actor.fclient.sender.amount", 1)

  private def initFqSendRouter(execActorAmount: Int, routerName: String = "fqSendRouter") = {
    val router = context.actorOf(RoundRobinPool(execActorAmount,
      supervisorStrategy = fqRouterEscalator).props(Props[FqueueSendActor]), routerName)
    Some(context.watch(router))
  }

  private def initObtainRouter(execActorAmount: Int, routerName: String = "fqObtainRouter") = {
    val router = context.actorOf(RoundRobinPool(execActorAmount,
      supervisorStrategy = fqRouterEscalator).props(Props[FqueueObtainActor]), routerName)
    Some(context.watch(router))
  }

  override def preStart() = {
    this.fqObtainRouterOpt = this.initObtainRouter(this.obtainAmount)
    this.fqSendRouterOpt = this.initFqSendRouter(this.senderAmount)
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} started", flush = true)
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  private def routeFqSendTracks(fqName: String, track: String, fqSendRouterOpt: Option[ActorRef]) = {
    fqSendRouterOpt match {
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} fqueueSendRouter never initialized")
      case Some(fqSendRouter) => fqSendRouter ! EsjTrack(fqName, track)
    }
  }

  private def stopFqSendRouter(fqSendRouterOpt: Option[ActorRef]) = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
  }

  /**
    * 触发fqueue, 获取数据给dcp router
    * @param fqName 队列名字
    * @param routerOpt dcp 路由
    * @return
    */
  private def broadcast2ObtainWebTracks(fqName: String, routerOpt: Option[ActorRef]) = {
    routerOpt match {
      case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} fqObtainRouter never initialized")
      case Some(router) => router ! Broadcast(FqueueName(fqName))
    }
  }

  /**
    * 必须把消息的名字改过来 toDo
    * @return
    */
  def receive = {
    case EsjTrack(fqName, track) => this.routeFqSendTracks(fqName, track, this.fqSendRouterOpt) //发送数据给esj
    case FqueueName(fqName) => this.broadcast2ObtainWebTracks(fqName, this.fqObtainRouterOpt) //广播获取web tracks
    case FqueueData(webTracks) => dcpRouter ! FqueueData(webTracks)
    case LastData(qName) => this.broadcast2ObtainWebTracks(qName, this.fqObtainRouterOpt)
    case codemapData: LastFqueueData => codemapActor forward codemapData  //发送最后一个数据个codemap actor
    case Stop => this.stopFqSendRouter(this.fqSendRouterOpt)
    case msg => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}

trait FqueueClientTool {
  private var builder: Option[XMemcachedClientBuilder] = None
  protected var client: Option[MemcachedClient] = None

  protected def initFqueueClientActor(address: String, connectionPoolSize: Int, connectTimeout: Int) = {
    builder = Some(new XMemcachedClientBuilder(AddrUtil.getAddresses(address)))
    builder foreach { _.setConnectionPoolSize(connectionPoolSize) }
    builder foreach { _.setConnectTimeout(connectTimeout) }
    client = Some(builder.get.build())
  }
}

class FqueueObtainActor extends Actor with FqueueClientTool {
  private lazy val address = this.serviceHost
  private lazy val connectionPoolSize = this.connectionPoolAmount
  private lazy val connectTimeout = this.timeoutOfConnection
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")

  private def serviceHost = {
    DynConfiguration.getStringOrElse("fqueue.host", "localhost:18740")
  }

  private def connectionPoolAmount = {
    val size = DynConfiguration.getIntOrElse("fqueue.connection.poolsize", 1)
    if (size > 0) size else 1
  }

  private def timeoutOfConnection: Int = {
    import dcp.common.BaseTypeImpl.longToInt
    ConfigurationEngine.getTime2Millisecond("fqueue.connection.timeout", 4) //默认4秒
  }

  override def preStart() {
    try{
      this.initFqueueClientActor(address, connectionPoolSize, connectTimeout)
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, ex.getStackTraceString)
        val info = s"${self.path} Create Mem cached Client or XMem cached Client Builder have a err"
        throw new InitFqueueClientException(info)
    }
  }

  //释放资源
  override def postStop() {
    client foreach { _.shutdown() }
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped and Release resources", flush = true)
  }

  private def getQueue(qName: String): Option[String] = {
    try {
      val str = client.get.get[String](qName)
      if (str == null || str.equals("null"))  None
      else Some(str)
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, ex.getStackTraceString)
        throw new GetQueueException(s"${self.path}  Get data from FQueue")
    }
  }

  private def obtainData(qName: String) = {
    var stop = false
    while (!stop) {
      this.getQueue(qName) match {
        case Some(str) => sender() ! FqueueData(Some(str))
        case None => stop = true
      }
    }
  }

  /**
   * 将队列里最后一个数据返回
   * 这样的后果是，非最后一个的所有数据会被服务器丢弃
   * 如果在获取数据的同时，又有数据插入，结果是不可预料的
   * @param qName　队列名字
   */
  private def obtainLastData(qName: String) = {
    var stop = false
    var lastData = Unit.toString()

    while (!stop) {
      this.getQueue(qName) match {
        case Some(str) => lastData = str
        case None => stop = true
      }
    }

    if (lastData != Unit.toString()) sender() ! LastFqueueData(Some(lastData))
  }

  def receive = {
    case FqueueName(qName) => this.obtainData(qName)
    case LastData(qName) => this.obtainLastData(qName)
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}

class FqueueSendActor extends Actor with FqueueClientTool {
  private lazy val address = "localhost:18740"
  private lazy val connectionPoolSize = 2
  private lazy val connectTimeout = 6000
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")

  override def preStart() {
    try {
      this.initFqueueClientActor(address, connectionPoolSize, connectTimeout)
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, ex.getStackTraceString)
        val info = s"${self.path} Create Mem cached Client or XMem cached Client Builder have a err"
        throw new InitFqueueClientException(info)
    }
  }

  //释放资源
  override def postStop() {
    client foreach { _.shutdown()}
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped and Release resources", flush = true)
  }

  private def sendTrack(qName: String, data: String): Boolean = {
    try {
      client.get.set(qName, 0, data)
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, ex.getStackTraceString)
        throw new SendQueueException(s"${self.path} Send data to FQueue")
    }
  }

  private def sendTracks(qName: String, tracks: TraversableOnce[String]) = {
    tracks foreach { str =>
      this.sendTrack(qName, str)
    }
  }

  def receive = {
    case EsjTracks(qName, tracks) => this.sendTracks(qName, tracks)
    case EsjTrack(qName, track) => this.sendTrack(qName, track)
    case _ => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} received unknown message")
  }
}
