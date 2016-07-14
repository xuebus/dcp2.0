/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序是dcp系统的清洗模块，也是业务的核心部分。
 *
 * 将原生的web用户轨迹转化为hbase actor、oryx actor、esj actor等actor 模块的输入数据
 *
 * 提取轨迹的item评分、tags评分 给oryx actor
 *
 * 提取轨迹里每个页面的dur、tags组成BrevityPageInfos 供hbase actor、esj actor使用
 * 需要注意的是页面的dur如果没有或者为0时则默认为1
 *
 * 在提取item和tags时需要依赖codemap模块
 * 在提取页面tags时需要切词提取，最终页面的tags是去重的。
 *
 * 下面是一个比较完整的原生用户数据
 *
 * {"uid":"a6ca9cb5a37a08cb","mid":"hhhhhhds","viewt":"1442198224","ip":"127.0.0.1","res":"2560x1600",
 * "trackers":[{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd302236?spm=0.0.0.0.g9kry1&scene=taobao_shop",
 * "title":"官方直售","viewtime":"10:39:33","id":"rd302236","vtype":"page","created":1442198373,"action":"v"},
 * {"ref":"http://dev.ec61.com/","url":"http://dev.ec61.com/page/rd336598?spm=0.0.0.0.g9kry1&scene=taobao_shop",
 * "title":"会员尊享","viewtime":"10:39:32","id":"rd336598","vtype":"page",
 * "created":1442198372,"pageTime":1,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd990357?spm=0.0.0.0.g9kry1&scene=taobao_shop",
 * "title":"人气面膜","viewtime":"10:39:30","id":"rd990357","vtype":"page","created":1442198371,
 * "pageTime":1,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/cate/710710130.htm?spm=0.0.0.0.g9
 * kry1&search=y&parentcatid=710710126&parentcatname=%b5%a5%c6%b7%d6%d6%c0%e0&catname=%d1%db%b2%bf%bb%a4%c0%ed#bd",
 * "title":"error","viewtime":"10:39:17","id":"710710130.htm","vtype":"cate","created":1442198358,
 * "pageTime":13,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/search.htm?spm=0.0.0.0.g9kry1","title":"error",
 * "viewtime":"10:39:15","id":"tb-45255382202","vtype":"item","created":1442198355,"pageTime":3,"action":"v"},
 * {"ref":"http://dev.ec61.com/","url":"http://dev.ec61.com/page/rd766698?spm=0.0.0.0.g9kry1&scene=taobao_shop",
 * "title":"镇店之宝","viewtime":"10:39:12","id":"rd766698","vtype":"page","created":1442198353,
 * "pageTime":2,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd296921?spm=0.0.0.0.g9kry1&scene=taobao_shop",
 * "title":"防晒面膜","viewtime":"10:39:9","id":"rd296921","vtype":"page","created":1442198350,
 * "pageTime":3,"action":"f"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd296921?spm=0.0.0.0.g9kry1&scene=taobao_shop",
 * "title":"防晒面膜","viewtime":"10:39:9","id":"rd296921","vtype":"page","created":1442198349,
 * "pageTime":1,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd766698.htm?spm=0.0.0.0.jfuhl2&scene=taobao_shop",
 * "title":"error","viewtime":"10:39:3","id":"rd766698.htm","vtype":"page","created":1442198344,
 * "pageTime":5,"action":"s"},{"ref":"","url":"http://dev.ec61.com/page/rd766698","title":"镇店之宝",
 * "viewtime":"10:39:2","id":"rd766698","vtype":"page","created":1442198342,"pageTime":2,"action":"v"},
 * {"ref":"","url":"http://dev.ec61.com/page/rd766698","title":"镇店之宝","viewtime":"10:39:1","id":"rd766698",
 * "vtype":"page","created":1442198341,"pageTime":1,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd766698.htm?spm=0.0.0.0.jfuhl2&scene=taobao_shop","title":"error",
 * "viewtime":"10:38:58","id":"tb-45255382202","vtype":"item","created":1442198339,"pageTime":2,"action":"v"},
 * {"ref":"http://dev.ec61.com/","url":"http://dev.ec61.com/page/rd766698.htm?spm=0.0.0.0.jfuhl2&scene=taobao_shop",
 * "title":"error","viewtime":"10:38:58","id":"rd766698.htm","vtype":"page","created":1442198338,
 * "pageTime":1,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd766698.htm?spm=0.0.0.0.jfuhl2&scene=taobao_shop",
 * "title":"一支淡化细纹、紧肤无敌的护肤品的乳清蛋白","viewtime":"10:38:57","id":"rd766698.htm",
 * "vtype":"item","created":1442198337,"pageTime":1,"action":"v"},{"ref":"http://dev.ec61.com/",
 * "url":"http://dev.ec61.com/page/rd766698.htm?spm=0.0.0.0.jfuhl2&scene=taobao_shop","title":"error",
 * "viewtime":"10:37:4","id":"rd766698.htm","vtype":"page","created":1442198225,"pageTime":112,"action":"v"},
 * {"ref":"http://dev.ec61.com/","url":"http://dev.ec61.com/page/rd766698.htm?spm=0.0.0.0.jfuhl2&scene=taobao_shop",
 * "title":"error","viewtime":"10:37:3","id":"rd766698.htm","vtype":"page",
 * "created":1442198224,"pageTime":1,"action":"v"}]}
 * 请使用json工具格式化后再看
 */

package dcp.business.service

import scala.collection.mutable.{Map => MuMap}
import scala.collection.immutable.Map

import org.ansj.domain.Term
import org.json4s._
import org.json4s.jackson.JsonMethods._

import dcp.business.codemap.{StringCodeMap, TraversableCodeMap, CodeMap}

/**
  * PageInfos 是记录一个页面清洗过后的信息
  * @param pageid 页面id
  * @param pageTags 页面的标签
  * @param ptype 页面类型, cate(类目页), item(商品页), page(页面), tag(优买吧的标签页)
  * @param dur 停留时长
  */
case class PageInfos(pageid: String, pageTags: Array[String], dur: String,
                     refOpt: Option[String], createdTime: String, ptype: String)

/**
  * 对PageInfos简单的封装
  * @param pageTags　已经去重的tags
  * @param dur 页面停留时间
  * @param ptype 页面类型, cate(类目页), item(商品页), page(页面), tag(优买吧的标签页)
  */
case class BrevityPageInfos(pageTags: Array[String], dur: String, ptype: String)

/**
  * 清洗数据的核心类
  * 处理清洗的业务逻辑
  */
class CleanMachine {
  implicit val formats = DefaultFormats   // Brings in default date formats etc.
  private lazy val logBase: Float = 4 //toDO 走动态配置
  private lazy val defaultReffer = "www.youmaiba.com" // toDO 走动态配置
  private lazy val defaultViewType = "v"  // toDO 走动态配置
  private lazy val defaultMid = "0" // toDO 走动态配置
  /**
    * PageTrack 是记录一个页面轨迹没有清洗过的信息
    * @param ref reffer
    * @param title 页面标题
    * @param pageid 页面id
    * @param viewType 页面类型
    * @param createdTime 浏览时间戳
    * @param pageDur 停留时长
    * @param action action (s, v, f)
    */
  private[CleanMachine] case class PageTrack(ref: Option[String],
                                             title: Option[String], pageid: String, viewType: String,
                                             createdTime: String, pageDur: String, action: Option[String])
  private[this] object viewTypeJudge {
    def unapply(viewType: String): Boolean = viewType != "tag"
  }

  /**
    * 用于统计一条页面轨迹中tags出现的次数
    */
  private val tagsSeqop = (result: MuMap[String, Int], tagsCount: (String, Int)) => {
    val newElem = (tagsCount._1, result.getOrElse(tagsCount._1, 0) + tagsCount._2)
    result += newElem
  }

  private val tagsCombop = (result1: MuMap[String, Int], result2: MuMap[String, Int]) => result1 ++= result2

  /**
    * 提取浏览轨迹部分, 需要外部处理json库发生的异常
    * @param userTracksJs web 轨迹
    * @return
    */
  private def parsingUserTracks(userTracksJs: JValue): Option[Array[JValue]] = {
    (userTracksJs \\ "trackers").extractOpt[Array[JValue]]
  }

  private def uid(userTracksJs: JValue): String = {
    val uidOpt = (userTracksJs \\ "uid").extractOpt[String]
    //获取用户id，如果一条轨迹没有用户id，认为是不符合的
    uidOpt match {
      case None => throw new Exception("can't found uid in track")
      case Some(uid) => if (uid.length > 0) uid else throw new Exception(" uid is void in track")
    }
  }

  /**
    * 用户进入网站的时间戳
    * @param userTracksJs web轨迹原生数据的json对象
    * @return
    */
  private def browseTime(userTracksJs: JValue): String = {
    val timeOpt = (userTracksJs \\ "viewt").extractOpt[String]
    timeOpt match {
      case None => BoDaoCommon.Date.DateTool.currentDate2Second
      case Some(time) => time
    }
  }

  /**
    * 如果mid不存在,则默认为 this.defaultMid
    * @param userTracksJs 用户轨迹
    * @return
    */
  private def emailId(userTracksJs: JValue): String = {
    val midOpt = (userTracksJs \\ "mid").extractOpt[String]
    midOpt match {
      case None => this.defaultMid
      case Some(mid) => if (mid.length > 0) mid else this.defaultMid  //mid为空的时候设置为默认值
    }
  }

  /**
    * 提取用户页面轨迹的id，如果找不到这个id，认为这条轨迹是有问题的
    * @param track 页面轨迹
    * @return
    */
  private def tid(track: JValue): String = {
    val tidOpt = (track \\ "id").extractOpt[String]
    tidOpt match {
      case None => throw new Exception("can't found track id in track")
      case Some(tid) => if (tid != "") tid else throw new Exception("can't found track id in track")
    }
  }

  private def viewType(track: JValue): String = {
    val viewTypeOpt = (track \\ "vtype").extractOpt[String]
    viewTypeOpt match {
      case None => throw new Exception("can't found track viewtype in track")
      case Some(viewType) => viewType
    }
  }

  private def createdTime(track: JValue): String = {
    val createdTimeOpt = (track \\ "created").extractOpt[String]
    createdTimeOpt match {
      case None => throw new Exception("can't fount track created time in track")
      case Some(createdTime) => createdTime
    }
  }

  private def savePageAction(actions: MuMap[String, Int], actionOpt: Option[String]) = {
    var default = "v"  //如果action找不到则默认为v

    actionOpt match {
      case None =>
      case Some(action) => default = action
    }

    if (actions.contains(default)) actions(default) += 1
    else actions += (default -> 1)
  }

  private def reffer(cleanedTrackInfos: MuMap[String, PageInfos]): String = {
    val pageInfos = cleanedTrackInfos.values
    val sort = (p1: PageInfos, p2: PageInfos) => {
      if (p1.createdTime < p2.createdTime) true
      else false
    }

    /* 以时间降序排序，第一个就是要的reffer */
    /* 如果没有就使用默认的reffer, 这是因为hbase是以列来存储数据的 */
    if (pageInfos.nonEmpty) {
      val sortInfo = pageInfos.toArray.sortWith(sort)
      sortInfo(0).refOpt match {
        case None => this.defaultReffer
        case Some(ref) => if (ref != "") ref else this.defaultReffer
      }
    }else this.defaultReffer
  }

  /**
    * 提取页面信息
    * @param track 每个页面的轨迹的信息
    * @return
    */
  private def extractPageTrack(track: JValue): PageTrack = {
    val id = this.tid(track) //浏览页面的id（其实就是路径）
    val viewType = this.viewType(track)  //浏览页面的类型（page, item, cate, 其中对于page来说，"<front>" 为首页)
    val createdTime = this.createdTime(track)  //用户打开页面的时间
    val ref = (track \\ "ref").extractOpt[String] //进入网站的reffer
    /* action，进入页面的动作类型（v => view, s => search(搜索), f => follow(关注)）*/
    val action = (track \\ "action").extractOpt[String]
    val pageDur = (track \\ "pageTime").extractOpt[String].getOrElse("1")  //页面停留时间, 最少1秒
    val title = (track \\ "title").extractOpt[String] //页面标题，用来切词分析

    PageTrack(ref, title, id, viewType, createdTime, pageDur, action)
  }

  /**
    * 合并页面重复的tags, 提取简短的页面信息
    * @param cleanedTrackInfos 页面信息
    * @return key 为pageid_time， value 为BrevityPageInfos， 保存了这个页面的停留时间和tags
    */
  private def getBrevityPageInfos(cleanedTrackInfos: MuMap[String, PageInfos]): Map[String, BrevityPageInfos] = {
    cleanedTrackInfos.mapValues(pageInfo =>
      BrevityPageInfos(pageInfo.pageTags.distinct, pageInfo.dur, pageInfo.ptype)
    ).toMap
  }

  /**
    * 计算item页面的评分
    * @param itemsCount 页面访问次数
    * @return
    */
  private def calculateItemScores(itemsCount: MuMap[String, Int]): Map[String, Float] = {
    import scala.math.log
    //加1是为了让取对数后不统一为0（可能大部分数据产生的真数都为1）
    itemsCount.mapValues(value =>
      (log(value + 1) / log(logBase)).toFloat //解决同一个商品由于多次访问而产生统计次数过多
    ).toMap
  }

  private def calculateTagsScores(tagsCount: MuMap[String, Int]): Map[String, Float] = {
    import scala.math.log
    tagsCount.mapValues(value =>
      (log(value + 1) / log(logBase)).toFloat
    ).toMap
  }

  /**
    * 记录每个item出现的次数
    * @param itemsCount 存储结果数据
    * @param item 要储存的item名字
    */
  private def itemsCounter(itemsCount: MuMap[String, Int], item: String) = {
    if (itemsCount.contains(item)) itemsCount(item) += 1
    else itemsCount += (item -> 1)
  }

  /**
    * 统计一条轨迹的tags的出现次数
    * @param cleanedTrackInfos 清洗过的页面信息
    * @return
    */
  private def tagsCounter(cleanedTrackInfos: MuMap[String, PageInfos]): MuMap[String, Int] = {
    val pageInfos = cleanedTrackInfos.values
    var tags = Array[String]()

    pageInfos foreach { elem =>
      if (elem.pageTags.nonEmpty) tags = tags ++: elem.pageTags
    }

    /* 对tags进行统计 */
    tags.map(elem => (elem, 1)).aggregate(MuMap[String, Int]())(tagsSeqop, tagsCombop)
  }

  /**
    * 利用切词提出title中隐藏的tags
    * @param title 页面的title
    * @return
    */
  private def extractTagsFromPageTitle(title: String, codemaps: Map[String, CodeMap]): Array[String] = {
    //去除在codemap中找不到的None元素
    def tagFilter(tag: Option[String]): Boolean = {
      tag match {
        case None => false
        case Some(str) => true
      }
    }

    /**
      * 切词
      * @param title 页面标题
      * @param codemap tags 的codemap
      * @return
      */
    def extractTags(title: String, codemap: StringCodeMap): Array[String] = {
      val wordsOpt = AnsjService.strParsing(title)
      wordsOpt match {
        case None => Array[String]()
        case Some(wordsTerm) =>
          val words = wordsTerm.toArray().filter(elem =>
            AnsjNatureService.containsNature(elem.asInstanceOf[Term].getNatureStr)  //词性不对的去掉
          )
          words.map(word =>
            codemap.getCode(word.asInstanceOf[Term].getName)
          ).filter(elem => tagFilter(elem)).map(elem => elem.get)
      }
    }

    codemaps.get("tags") match {
      case None => Array[String]()
      case Some(codemap) =>
        val stringCodeMap = codemap.asInstanceOf[StringCodeMap]
        extractTags(title, stringCodeMap)
    }
  }

  /**
    * 通过codemap获取页面对应的tagscodes
    * @param pageTrack 页面轨迹信息
    * @param codemaps codemaps
    * @return
    */
  private def extractPageTagsFromCodeMap(pageTrack: PageTrack, codemaps: Map[String, CodeMap]): Array[String] = {
    codemaps.get(pageTrack.viewType) match {
      case None => Array[String]()
      case Some(codemap) =>
        val traversableCodeMap = codemap.asInstanceOf[TraversableCodeMap]
        traversableCodeMap.getCode(pageTrack.pageid) match {
          case None => Array[String]()
          case Some(tagCodes) => tagCodes.toArray
        }
    }
  }

  /**
    * 真正实现清洗功能的函数
    * @param userTracksJs web 轨迹的json
    * @param browseTracks 浏览轨迹部分的json
    */
  private def cleanUserTracks(userTracksJs: JValue,
                              browseTracks: Array[JValue], codemaps: Map[String, CodeMap]): UserTracks = {
    val uid = this.uid(userTracksJs)  //用户的id
    val browseTime = this.browseTime(userTracksJs) //浏览的时间戳
    val mid = this.emailId(userTracksJs)  //邮件反馈id

    val itemsCount = MuMap[String, Int]() //item 的评分, key 为pageid(item名字)
    /* actions 页面类型和次数，进入页面的动作类型（v => view, s => search(搜索), f => follow(关注)）*/
    val actions = MuMap[String, Int]()
    var totalDur: Int = 0 //浏览总时间
    /* cleanedTrackInfos的key为每个页面的id， value为这个页面的信息，其中tags(可能有重复) */
    val cleanedTrackInfos = MuMap[String, PageInfos]()
    var ref = Unit.toString()  //轨迹的reffer

    browseTracks foreach { browseTrack =>
      try {
        val extractedPageTrack = this.extractPageTrack(browseTrack)
        var pageTags = Array[String]()

        extractedPageTrack.viewType match {
          //优买吧专属, pageid 就是标签页的tag。注：每个标签页只有一个标签
          case "tag" => pageTags = pageTags ++: Array(extractedPageTrack.pageid)
          case vtype @ viewTypeJudge() => //处理item以及其他类型的要处理要处理的步骤
            if (vtype == "item") this.itemsCounter(itemsCount, extractedPageTrack.pageid) //记录item评分
            /* 获取标题, 对标题切词, 去除重复的tag */
            extractedPageTrack.title match {
              case None =>
              case Some(title) => pageTags = this.extractTagsFromPageTitle(title, codemaps)
            }
            /* 根据页面的类型获取对应的codemap, 然后提取tags */
            pageTags = pageTags ++: this.extractPageTagsFromCodeMap(extractedPageTrack, codemaps)
        }

        val pageInfo = PageInfos(extractedPageTrack.pageid, pageTags, extractedPageTrack.pageDur,
          extractedPageTrack.ref, extractedPageTrack.createdTime, extractedPageTrack.viewType)
        /* pageTags中的tags非常可能有重复，以页面id_页面创建时间组合成key */
        cleanedTrackInfos += (s"${extractedPageTrack.pageid}_${extractedPageTrack.createdTime}" -> pageInfo)

        /* 由于 pageDur 设定至少1s，所以可能会跟元数据计算出来的有区别 */
        totalDur += extractedPageTrack.pageDur.toInt
        /* 统计轨迹的action */
        this.savePageAction(actions, extractedPageTrack.action)
      } catch {
        case ex: Exception => println(ex.getStackTraceString) /* toDo 删除输出 */
      }
    }

    ref = this.reffer(cleanedTrackInfos)
    val itemScore = this.calculateItemScores(itemsCount)
    val tagsScore = this.calculateTagsScores(this.tagsCounter(cleanedTrackInfos))
    val brevityPageInfos = this.getBrevityPageInfos(cleanedTrackInfos)
    UserTracks(uid, browseTime, mid, actions.toMap, totalDur, ref, itemScore, tagsScore, brevityPageInfos)
  }

  /**
    * 清洗功能的入口, 在提取json值时会抛出异常，外部系统需要捕获这些异常
    * @param userTrackStr web轨迹原生数据
    */
  def clean(userTrackStr: String, codemaps: Map[String, CodeMap]): Option[UserTracks] = {
    val userTracksJs = parse(userTrackStr)
    val browseTracksOpt = this.parsingUserTracks(userTracksJs)
    browseTracksOpt match {
      case None => None
      case Some(browseTracks) => Some(this.cleanUserTracks(userTracksJs, browseTracks, codemaps))
    }
  }
}