/**
 * Copyright (c) 2016, BoDao, Inc. All Rights Reserved.
 *
 * Author@ cwx
 *
 * 本程序实现 oryx actor 分别监管两个子actor：item actor和tags actor，用这两个actor来
 * 接收处理之后的item数据和tags数据。item actor接收到数据之后会缓存到buffer中，当buffer
 * 达到最大存储量就导入文件，文件会定时上传到oryx中（当接收到Flush信息时）。item actor
 * 运用了akka的FSM有限状态机，有三种状态：Start表示初始状态，Run表示运行状态，Full表示
 * buffer已满的状态。除此之外actor还会将dcp处理的数据永久缓存到本地中。tags actor与
 * item actor类似。
 */

package dcp.business.actors

import java.io.{File, FileInputStream}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import BoDaoCommon.Date.DateTool
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, FSM, Props}
import dcp.business.service.{TracksFormattingTool, UserTracks}
import dcp.common._
import dcp.config.{ConfigurationEngine, DynConfiguration}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, InputStreamEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}

class ResponseExcetion(message: String) extends Exception(message) //把文件数据推到oryx返回状态
case class OryxContent(str: String) extends FSMContent[String](str) //接收数据
case class OryxBufferCache(bufferCache: ArrayBuffer[String]) extends FSMBuffer[String](bufferCache)

class OryxActor extends Actor {
  private var itemActorOpt: Option[ActorRef] = None
  private var tagsActorOpt: Option[ActorRef] = None
  private val logActor = context.actorSelection("/user/DcpSystemActor/logActor")

  override def preStart() = {
    val itemActor = context.actorOf(Props[ItemActor], "itemActor")
    val tagsActor = context.actorOf(Props[TagsActor], "tagsActor")
    this.itemActorOpt = Some(context.watch(itemActor))
    this.tagsActorOpt = Some(context.watch(tagsActor))
  }

  override def postStop() = {
    logActor ! LogMsg(LogLevelEnum.Info, s"${self.path} Stoped", flush = true)
  }

  /**
   * 把处理完的数据发给itemActor
   * @return
   */
  private def sendItemData(cleanedUserTrack: UserTracks, itemActorOpt: Option[ActorRef]): Boolean = {
    val tracks = TracksFormattingTool.formattingTrackItemScore(cleanedUserTrack)
    tracks.foreach(tracks => {
      itemActorOpt match {
        case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} itemActor never initialized")
        case Some(itemActor) => itemActor ! OryxContent(s"$tracks\n")
      }
    })
    true
  }

  /**
   * 把处理完的数据发给tagsActor
   * @return
   */
  private def sendTagsData(cleanedUserTrack: UserTracks, tagsActorOpt: Option[ActorRef]): Boolean = {
    val tracks = TracksFormattingTool.formattingTrackTagsScore(cleanedUserTrack)
    tracks.foreach(tracks => {
      tagsActorOpt match {
        case None => logActor ! LogMsg(LogLevelEnum.Err, s"${self.path} tagsActor never initialized")
        case Some(tagsActor) => tagsActor ! OryxContent(s"$tracks\n")
      }
    })
    true
  }

  private def stopOryxActor: Boolean = {
    logActor ! LogMsg(LogLevelEnum.Info, s"Start to Stop ${self.path}", flush = true)
    context.stop(self)
    true
  }

  def receive = {
    case CleanedTrack(cleanedUserTrack) =>
      this.sendItemData(cleanedUserTrack, this.itemActorOpt)
      this.sendTagsData(cleanedUserTrack, this.tagsActorOpt)

    case Stop => this.stopOryxActor
    case _ => logActor ! LogMsg(LogLevelEnum.Err, "received unknown message")
  }
}

trait OryxTrait extends Actor with FSM[FSMState, FSMData] {
  import BoDaoCommon.File.LocalFileTool
  private lazy val logActor = context.actorSelection("/user/DcpSystemActor/logActor")
  private lazy val cache = ArrayBuffer[String]()
  private lazy val cacheRoot = this.cacheRootDir
  private lazy val cleanedRoot = this.cleanedRootDir
  private var oryxFileName = ""
  protected val service: String
  protected val maxStorage: Int
  protected val name: String

  private def cacheRootDir = {
    ConfigurationEngine.getDir("cache.dir", "./data")
  }

  private def cleanedRootDir = {
    ConfigurationEngine.getDir("cache.cleaned.dir", "cleaned")
  }

  /**
   * 通过时间戳来获取oryx缓存文件名
   * @return
   */
  private def getOryxFileName = {
    s"${this.cacheRoot}/${this.cleanedRoot}/tmp/${this.name}-${DateTool.currentMillis}"
  }

  /**
   * 通过时间戳来获取本地缓存文件名
   * @return
   */
  private def getLocalFileName(date: String) = {
    s"${this.cacheRoot}/${this.cleanedRoot}/$date/${this.name}-$date"
  }

  private def createLocalDir4Day(date: String): Boolean = {
    val dir = s"${this.cacheRoot}/${this.cleanedRoot}/$date"
    if (!LocalFileTool.exists(dir)) LocalFileTool.createDir(dir)
    else true
  }

  private def saveData2File(fileName: String, buffer: ArrayBuffer[String], append: Boolean = true): Boolean = {
    LocalFileTool.save(fileName, buffer, append) match {
      case false => throw new Exception(s"${self.path} save data to file hava a err")
      case true => true
    }
  }

  private def deleteFile(fileName: String): Boolean = {
    LocalFileTool.deleteRegFile(fileName) match {
      case false => throw new Exception(s"${self.path} delete file hava a err")
      case true => true
    }
  }

  /**
   * 把buffer内容导入文件,把文件内容上传到oryx,删除文件
   */
  private def sendData2Oryx(fileName: String, buffer: ArrayBuffer[String], service: String, append: Boolean = true): Boolean = {
    try {
      this.saveData2File(fileName, buffer, append) //把buffer内容导入文件
      HttpTool.postData2Oryx(fileName, service) //把文件内容上传到oryx
      this.deleteFile(fileName) //删除文件
      true
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path}, ${ex.getStackTraceString}")
        false
    }
  }

  /**
   * 把数据缓存到本地
   */
  private def saveData2Local(buffer: ArrayBuffer[String], append: Boolean = true): Boolean = {
    try {
      val date = BoDaoCommon.Date.DateTool.currentDate2Day
      if (!this.createLocalDir4Day(date)) return false
      this.saveData2File(this.getLocalFileName(date), buffer, append) //把buffer内容导入文件
      true
    } catch {
      case ex: Exception =>
        logActor ! LogMsg(LogLevelEnum.Err, s"${self.path}, ${ex.getStackTraceString}")
        false
    }
  }

  /**
   * 为初始化的状态
   */
  startWith(FSMStart, FSMUninitialized)

  when(FSMStart) {
    case Event(FSMInit, FSMUninitialized) => stay using OryxBufferCache(cache)
    case Event(OryxContent(str), oryxBufferCache @ OryxBufferCache(buffer)) =>
      this.oryxFileName = this.getOryxFileName //新建文件
      goto(FSMRunning) using OryxBufferCache(bufferCache = buffer :+ str)

    case Event(FSMFlush, oryxInternalData @ OryxBufferCache(buffer)) =>
      stay using OryxBufferCache(buffer)
  }

  when(FSMRunning) {
    case Event(OryxContent(str), oryxBufferCache @ OryxBufferCache(buffer)) =>
      buffer += str
      buffer.length match {
        case this.maxStorage => goto(FSMFill) using oryxBufferCache.copy(bufferCache = buffer)
        case _ => stay using oryxBufferCache.copy(bufferCache = buffer)
      }

    case Event(FSMFlush, oryxBufferCache @ OryxBufferCache(buffer)) =>
      this.sendData2Oryx(this.oryxFileName, buffer, this.service) //把dcp处理的数据上传到oryx
      this.saveData2Local(buffer) //把dcp处理的数据缓存到本地
      buffer.clear()
      goto(FSMStart) using OryxBufferCache(buffer)
  }

  when(FSMFill) {
    case Event(OryxContent(str), oryxBufferCache @ OryxBufferCache(buffer)) =>
      LocalFileTool.save(this.oryxFileName, buffer, append = true) //把buffer内容导入文件
      this.saveData2Local(buffer)
      buffer.clear()
      buffer += str
      goto(FSMRunning) using oryxBufferCache.copy(bufferCache = buffer)

    case Event(FSMFlush, oryxBufferCache @ OryxBufferCache(buffer)) =>
      this.sendData2Oryx(this.oryxFileName, buffer, this.service)
      this.saveData2Local(buffer)
      buffer.clear()
      goto(FSMStart) using OryxBufferCache(buffer)
  }

  initialize()
}

class ItemActor extends OryxTrait {
  override val service = DynConfiguration.getStringOrElse("oryx.item.httpUrl", "")
  override val maxStorage = DynConfiguration.getIntOrElse("oryx.item.maxStorage", 1000)
  override val name = DynConfiguration.getStringOrElse("oryx.itemScore.fileName", "itemScore")
  private val timeInterval = DynConfiguration.getIntOrElse("oryx.item.TimeInterval", 10)

  /**
    * 初始化自己的内部状态
    */
  override def preStart() = {
    self ! FSMInit
  }

  setTimer("ItemTimer", FSMFlush, this.timeInterval.minutes, repeat = true)
}

class TagsActor extends OryxTrait {
  override val service = DynConfiguration.getStringOrElse("oryx.tags.httpUrl", "")
  override val maxStorage = DynConfiguration.getIntOrElse("oryx.tags.maxStorage", 10000)
  override val name = DynConfiguration.getStringOrElse("oryx.tagsScore.fileName", "tagsScore")
  private val TimeInterval = DynConfiguration.getIntOrElse("oryx.tags.TimeInterval", 5)

  /**
    * 初始化自己的内部状态
    */
  override def preStart() = {
    self ! FSMInit
  }

  setTimer("TagsTimer", FSMFlush, this.TimeInterval.minutes, repeat = true)
}

object HttpTool {
  /**
   * 把文件的数据上传到oryx中
   * @param fileName 上传的文件名
   * @param httpUrl oryx的url
   * @return
   */
  def postData2Oryx(fileName: String, httpUrl: String): Boolean = {
    val httpClient = HttpClients.createDefault
    try {
      val httpPost = new HttpPost(httpUrl)
      val file = new File(fileName)
      val reqEntity = new InputStreamEntity(new FileInputStream(file), -1, ContentType.create("text/csv"))
      reqEntity.setChunked(true)
      httpPost.setEntity(reqEntity)
      this.resposeStatus(httpPost, httpClient)
      true
    } finally {
      httpClient.close()
    }
  }

  private def resposeStatus(httpPost: HttpPost, httpClient: CloseableHttpClient): Boolean = {
    val response = httpClient.execute(httpPost)
    try {
      val status = response.getStatusLine.getStatusCode
      if(status != 204){
        val info = s"The response status is not 204"
        throw new ResponseExcetion(info)
      }
      true
    } finally {
      response.close()
    }
  }
}