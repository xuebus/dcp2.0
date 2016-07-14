/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供数据格式化的功能
 * TracksFormattingTool 类将清洗完的数据格式化为hbase, esj, oryx 系统需要的数据
 * 对于hbase, 主要进行格式化的是pageinfo部分和action部分
 */

package dcp.business.service

import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * dcp 清洗完的数据
  * @param uid 用户id
  * @param browseTime 进入系统的时间
  * @param mid 邮件反馈的id
  * @param actions 轨迹类型的次数统计，类型分为v，f， s
  * actions 页面类型和次数，进入页面的动作类型（v => view, s => search(搜索), f => follow(关注)）
  * @param totalDur 页面停留的总时间
  * @param ref 轨迹的reffer
  * @param itemsScore 产品评分
  * @param tagsScore 用户感兴趣的标签评分
  * @param brevPageInfos 用户轨迹的简要，主要保存了dur跟页面对应tags，便于供hbase actor和oryx actor 使用
  */
case class UserTracks(uid: String, browseTime: String, mid: String, actions: Map[String, Int],
                      totalDur: Int, ref: String, itemsScore: Map[String, Float],
                      tagsScore: Map[String, Float], brevPageInfos: Map[String, BrevityPageInfos])

/**
  * 将清洗过的数据格式化成hbase 表中数据需要的格式
  * @param uid 用户id
  * @param actions 轨迹类型的次数统计，类型分为v，f， s
  * @param totalDur 页面停留的总时间
  * @param pageInfos 页面信息, 也是最主要格式化的部分, 包括每个页面的dur和tags
  * @param ref 轨迹的reffer
  * @param browseTime 进入系统的时间
  * @param mid 邮件反馈的id
  */
case class TrackInHbase(uid: String, actions: Option[String], totalDur: String,
                        pageInfos: Option[String], ref: String, browseTime: String, mid: String)

/**
  * 提供解析UserTracks 类中某些字段的工具类
  */
object TracksFormattingTool {
  private type BrevPageInfosTuple = (String, BrevityPageInfos)
  private type Score = (String, Float)

  private def formattingPageInfo(brevPageInfos: Map[String, BrevityPageInfos]): Array[JValue] = {
    val formatting: (BrevPageInfosTuple) => JValue = (BPIT: BrevPageInfosTuple) => {
      BPIT._1 -> ("tags" -> BPIT._2.pageTags.toList) ~ ("dur" -> BPIT._2.dur.toInt) ~ ("ptype" -> BPIT._2.ptype)
    }
    (brevPageInfos map formatting).toArray
  }

  /**
    * 将轨迹里的页面信息格式化
    * 如果brevPageInfos 里没有内容, 不进行格式化,返回None
    * 在返回值为None的时候, 不应该将数据写入hbase
    * @param brevPageInfos 简短信息
    * @return
    */
  private def formattingPageInfo4Hbase(brevPageInfos: Map[String, BrevityPageInfos]): Option[String] = {
    /* 判空操作,是为了在pageinfos 为空的时候, 不将数据存入hbase */
    brevPageInfos.nonEmpty match {
      case false => None
      case true => Some(compact(render(this.formattingPageInfo(brevPageInfos).toList)))
    }
  }

  private def formattingAction(actions: Map[String, Int]): Option[String] = {
    actions.nonEmpty match {
      case false => None
      case true => Some(compact(render(actions)))
    }
  }

  /**
    * 将没有页面内容和action的轨迹去掉
    * @param pageInfos 页面内容
    * @param actions 页面action
    * @return
    */
  private def pageInfoAndActionFilter(pageInfos: Option[String], actions: Option[String]): Boolean = {
    (for {
      p <- pageInfos
      a <- actions
    } yield a) match {
      case None => false
      case Some(t) => true
    }
  }

  def formattingTrack4Hbase(cleanedUserTrack: UserTracks): Option[TrackInHbase] = {
    val cut = cleanedUserTrack
    cleanedUserTrack.brevPageInfos.nonEmpty match {
      case false => None
      case true =>
        val actionsOpt = this.formattingAction(cleanedUserTrack.actions)
        val pageInfos = this.formattingPageInfo4Hbase(cut.brevPageInfos)

        if (this.pageInfoAndActionFilter(pageInfos, actionsOpt)) {
          val totalDur = cut.totalDur.toString
          val trackInHbase = TrackInHbase(cut.uid, actionsOpt, totalDur, pageInfos, cut.ref, cut.browseTime, cut.mid)
          Some(trackInHbase)
        } else {
          None
        }
    }
  }

  private def formattingScore(uid: String, scoreMap: Map[String, Float],
                              timestamp: String): TraversableOnce[String] = {
    val formatting: (Score) => String = (score: Score) => {
      "%s,%s,%s,%s".format(uid, score._1, score._2, timestamp)
    }
    scoreMap map formatting
  }

  def formattingTrackItemScore(cleanedUserTrack: UserTracks): TraversableOnce[String] = {
    this.formattingScore(cleanedUserTrack.uid, cleanedUserTrack.itemsScore, cleanedUserTrack.browseTime)
  }

  def formattingTrackTagsScore(cleanedUserTrack: UserTracks): TraversableOnce[String] = {
    this.formattingScore(cleanedUserTrack.uid, cleanedUserTrack.tagsScore, cleanedUserTrack.browseTime)
  }

  /**
    * 将数据解压成jvalue的形式, esj actor将多个jvalue 转化成一个字符串发送给fqueue
    * @param cleanedUserTrack 清洗完的用户轨迹数据
    * @return
    */
  def formattingTrack4esj(cleanedUserTrack: UserTracks): Option[JValue] = {
    implicit val formats = DefaultFormats
    cleanedUserTrack.brevPageInfos.nonEmpty match {
      case false => None
      case true =>
        val pageInfos = this.formattingPageInfo(cleanedUserTrack.brevPageInfos)
        val actionsOpt = this.formattingAction(cleanedUserTrack.actions)
        actionsOpt match {
          case None => None
          case Some(actions) =>
            val track4esj = cleanedUserTrack.uid ->
              ("pageinfo" -> pageInfos.toList) ~
              ("viewtime" -> cleanedUserTrack.browseTime) ~
              ("duration" -> cleanedUserTrack.totalDur) ~
              ("actions" -> parse(actions))
            Some(track4esj)
        } //end actionsOpt match
    }
  }
}