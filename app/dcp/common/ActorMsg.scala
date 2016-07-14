package dcp.common

import akka.actor.ActorSystem

import dcp.business.codemap.CodeMap
import dcp.business.service.UserTracks
import dcp.common.LogLevelEnum.LogLevelEnum
import dcp.common.StatusEnum.StatusEnum

/**
  * 日志级别
  */
object LogLevelEnum extends Enumeration {
  type LogLevelEnum = Value
  val Info = Value
  val Warn = Value
  val Err = Value
}

/**
  * 状态类
  */
object StatusEnum extends Enumeration {
  type StatusEnum = Value
  val Success = Value
  val Fail = Value
}

/**
  * 系统默认消息
  */
case class SysStart(system: ActorSystem)
case class SysStop()
case class Status(status: StatusEnum, data: String)

/**
  * fqueue 模块消息
  */
/**
  * FqueueClientActor 向所有请求者返回这个消息， 没有数据时， data为None
  * FqueueClientActor 发给FqueueToolActor的消息，包含了拿到的数据
  * @param data 从队列拿的数据
  */
case class FqueueData(data: Option[String])
case class LastFqueueData(data: Option[String]) //队列最后一个数据, 用于codemap
case class FqueueInit()                       //DcpSystem 初始化系统时发给FqueueActor的消息

/**
  * 所有向FqueueClientActor发起拿数据请求都发送这条msg
  * @param name name为队列名字
  */
case class FqueueName(name: String)
/**
  * 所有向FqueueClientActor发起拿队列里最后的数据请求都发送这条msg
  * @param name name为队列名字
  */
case class LastData(name: String)

/**
  * 清洗好的用户轨迹，发送到esj队列
  * @param name 队列名字， 为了更好的扩展
  * @param tracks 清洗好的用户轨迹
  */
case class EsjTracks(name: String, tracks: TraversableOnce[String])
case class EsjTrack(name: String, track: String)

/**
  * Dcp 模块相关消息
  */
case class WebTrack(track: String)            //从fqueue拿出来的用户数据， FqueueToolActor 转发给DcpActor
case class UpdateCodeMap()                    //dcp actors　发送给自己进行定时更新，和发送给codemap actor触发更新
case class CodeMapData(codemaps: Map[String, CodeMap])  //CodeMapActor 发给DcpActor的codemap消息
case class CleanedTrack(userTracks: UserTracks) //dcp 清洗完的数据消息,数据保存在userTracks里

/**
  * log 模块消息
  */
case class LogMsg(lever: LogLevelEnum, log: String, flush: Boolean = false)  //flush 代表是否即使刷写
case class DeleteLog(lever: LogLevelEnum, date: String) //删除指定日期和类型的log
case class DeleteDayLogs(date: String)  //删除指定日期的所有类型的log

case class StatisticsMsg(visitors: Long, traffic: Long)