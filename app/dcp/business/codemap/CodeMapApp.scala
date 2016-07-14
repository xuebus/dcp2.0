/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序实现codemap的解析、持久化存储、启动加载操作的接口。
 * 原生codemap数据是json，需要解析，其内部分为以下几种：
 * 1、　"page": { "aaa": null, "bbb": ["4", "5", "6"], "ccc": ["7", "8", "9"], "ddd": ["1", "2"] }
 * ２、　"cate": { "hhh": ["11", "12", "13"], "qqq": ["14", "15", "16"], "rrr": ["17", "19"], "yyy": ["11", "12"]  }
 * ３、　"item": { "jjj": ["211", "212", "213"], "kkk": ["214", "216"],　"lll": ["217", "219"], "ooo": ["211", "212"] }
 * ４、　"itemInfo": { "item1001": { "sales": 1, "price": 10.11 },　"item1003": { "sales": 3, "price": 155.0  } }
 * ５、　"tags": { "1001": "包邮", "1002": "美白", "1003": "护肤" }
 * 其中前３种格式一样，可以抽象出来，后面两种单独实现
 */

package dcp.business.codemap

import org.json4s._
import org.json4s.jackson.JsonMethods._

import dcp.config.ConfigurationEngine

object CodeMapApp {
  /**
    * 保存了codemap里每种类型解析完的数据
    * @param name　codemap类型
    * @param values　解析完的数据
    */
  private[CodeMapApp] case class CodeMapTuple(name: String, values: TraversableOnce[String])
  private[CodeMapApp] object MapFormatsImpl {
    import scala.language.implicitConversions
    /**
      * 用来将json解析出来的Map转换成需要的字符串
      * @param codemapPart (name: String, data: Map)　json解析出来的Map, name为类型(page, item, cate)
      * data 为解析完的json数据
      * @return
      */
    implicit def mapToCodeMapTuple(codemapPart: (String, Map[String, List[String]])): Option[CodeMapTuple] = {
      val name = codemapPart._1
      val data = codemapPart._2
      val pile = (x: String, y: String) => { s"$x,$y" }
      val codemapValues = data.keys map { key  =>
        if (data(key).nonEmpty) s"$key\t${data(key).reduce(pile)}\n" else ""
      } filter { value => value.length > 0 }

      Some(new CodeMapTuple(name, codemapValues))
    }
  }

  /**
    * 用来提取相同格式的codemap 内容，如page, item, cate
    * @param name codemap 部分的名字，如page, item, cate
    * @param codemapJsons 已经转换为josn格式的codemap内容
    * @return
    */
  private def generalExtractCodeMap(name: String, codemapJsons: Map[String, JValue]): Option[CodeMapTuple] = {
    implicit val formats = DefaultFormats
    import MapFormatsImpl._
    val pageJsonsOpt = codemapJsons(name).extractOpt[Map[String, List[String]]]
    pageJsonsOpt match {
      case None => None
      case Some(pageJsons) => (name, pageJsons)
    }
  }

  /**
    * 提取page(页面) codemap
    * 每个页面都有其标签
    * page: { "aaa": null, "bbb": ["4", "5", "6"], "ccc": ["7", "8", "9"], "ddd": ["1", "2"] }
    * @param codemapJsons 转换为json类型的codemaps
    * @return
    */
  private def extractPageCodeMaps(codemapJsons: Map[String, JValue])
                                 (func: (String, Map[String, JValue]) => Option[CodeMapTuple]): Option[CodeMapTuple] = {
    func("page", codemapJsons)
  }

  /**
    * 提取cate(类目) codemap
    * 类目也像页面一样有自己的标签
    * "cate": { "hhh": ["11", "12", "13"], "qqq": ["14", "15", "16"],
    * "rrr": ["17", "18", "19"], "yyy": ["11", "12"]  }
    * @param codemapJsons　转换为json类型的codemaps
    * @return
    */
  private def extractCateCodeMaps(codemapJsons: Map[String, JValue])
                                 (func: (String, Map[String, JValue]) => Option[CodeMapTuple]): Option[CodeMapTuple] = {
    func("cate", codemapJsons)
  }

  /**
    * 提取item(商品) codemap
    * "item": { "jjj": ["211", "212", "213"], "kkk": ["214", "215", "216"],
    * "lll": ["217", "218", "219"], "ooo": ["211", "212"] }
    * @param codemapJsons 转换为json类型的codemaps
    * @return
    */
  private def extracItemCodeMaps(codemapJsons: Map[String, JValue])
                                (func: (String, Map[String, JValue]) => Option[CodeMapTuple]): Option[CodeMapTuple] = {
    func("item", codemapJsons)
  }

  /**
    * 提取iteminfo(商品信息，　销售量，价格) codemap
    * "itemInfo": { "item1001": { "sales": 1, "price": 10.11 },
    * "item1002": { "sales": 2, "price": 12.1 }, "item1003": { "sales": 3, "price": 155.0  } }
    * @param codemapJsons 转换为json类型的codemaps
    * @return
    */
  private def extracItemInfoCodeMaps(codemapJsons: Map[String, JValue]): Option[CodeMapTuple] = {
    implicit val formats = DefaultFormats
    val name = "itemInfo"
    val itemInfoJsonOpt = codemapJsons(name).extractOpt[Map[String, JValue]]
    itemInfoJsonOpt match {
      case None => None
      case Some(itemInfoJson) =>
        val extrac = (kvp: (String, JValue)) => {
          val price = (kvp._2 \\ "price").extractOrElse("0.0")
          val sales = (kvp._2 \\ "sales").extractOrElse("0")
          s"${kvp._1}\t$price,$sales\n"
        }
        val values = itemInfoJson map extrac
        Some(new CodeMapTuple(name, values))
    }
  }

  /**
    * 提取tag(标签) codemap
    * "tags": { "1001": "包邮", "1002": "美白", "1003": "护肤" }
    * @param codemapJsons 转换为json类型的codemaps
    * @return
    */
  private def extracTagsCodeMaps(codemapJsons: Map[String, JValue]): Option[CodeMapTuple] = {
    implicit val formats = DefaultFormats
    val name = "tags"
    val tagsJsonOpt = codemapJsons(name).extractOpt[Map[String, String]]
    tagsJsonOpt match {
      case None => None
      case Some(tagsJson) =>
        val reversal = (kvp: (String, String)) => { s"${kvp._2}\t${kvp._1}\n" }
        val values = tagsJson map reversal
        Some(new CodeMapTuple(name, values))
    }
  }

  /**
    * 将codemap分类型写入文件
    * @param cmt　保存解析完的codemap数据
    * @return
    */
  def saveCodeMap(cmt: CodeMapTuple): Boolean = {
    import BoDaoCommon.File.LocalFileTool
    import dcp.config.ConfigurationEngine
    val root = ConfigurationEngine.getDir("codemap.dir", "./DynConfig/codemap")
    LocalFileTool.save(s"$root/${cmt.name}", cmt.values, append = false)  //直接刷写，覆盖
  }

  /**
    * 持久化codemaps
    * @param cmtOpt 解析完的codemap part
    * @param persistenceFunc 实现存储数据的函数
    * @return
    */
  def persistenceCodeMaps(cmtOpt: Option[CodeMapTuple])
                         (persistenceFunc: CodeMapTuple => Boolean = this.saveCodeMap): Boolean = {
    cmtOpt match {
      case None => false
      case Some(cmt) => persistenceFunc(cmt)
    }
  }

  /**
    * 提取每个部分的的数据
    * @param codemapJsons 解析成json的codemap
    * @param codemapType 类型名字
    * @return
    */
  private def extracCodeMaps(codemapJsons: Map[String, JValue], codemapType: String): Option[CodeMapTuple] = {
    codemapType match {
      case "page" => this.extractPageCodeMaps(codemapJsons)(this.generalExtractCodeMap)
      case "item" => this.extracItemCodeMaps(codemapJsons)(this.generalExtractCodeMap)
      case "cate" => this.extractCateCodeMaps(codemapJsons)(this.generalExtractCodeMap)
      case "itemInfo" => this.extracItemInfoCodeMaps(codemapJsons)
      case "tags" => this.extracTagsCodeMaps(codemapJsons)
    }
  }

  /**
    * 解析json数据
    * 由于解析json可能出现异常，所以需要在外面捕捉异常
    * 不在这里捕捉异常的原因是，让外围更方便处理发生的异常
    */
  def parsingCodeMaps(codemapStr: String): Map[String, Option[CodeMapTuple]] = {
    var codeMapTuples = Map[String, Option[CodeMapTuple]]()
    implicit val formats = DefaultFormats   // Brings in default date formats etc.
    val codemapJsons = parse(codemapStr).extractOpt[Map[String, JValue]].get

    codemapJsons.keys foreach { codemapType =>
      val cmtOpt = this.extracCodeMaps(codemapJsons, codemapType)
      codeMapTuples = codeMapTuples + (codemapType -> cmtOpt)
    }
    codeMapTuples
  }

  private def loadingCodeMap(codeMaptype: String): CodeMap = {
    val root = ConfigurationEngine.getDir("codemap.dir", "./DynConfig/codemap")
    val defFile = s"$root/default/$codeMaptype"
    val dtFile = s"$root/$codeMaptype"

    /**
      * 分类实现加载codemap
      */
    codeMaptype match {
      case "page" => new TraversableCodeMap(defFile, dtFile, codeMaptype)
      case "cate" => new TraversableCodeMap(defFile, dtFile, codeMaptype)
      case "item" => new TraversableCodeMap(defFile, dtFile, codeMaptype)
      case "itemInfo" => new TraversableCodeMap(defFile, dtFile, codeMaptype)
      case "tags" => new StringCodeMap(defFile, dtFile, codeMaptype)
    }
  }

  def loadCodeMaps(): Map[String, CodeMap] = {
    var codemaps = Map[String, CodeMap]()
    val codeMaptypes = Array("page", "cate", "item", "tags", "itemInfo")
    codeMaptypes foreach { elem =>
      codemaps = codemaps + (elem -> this.loadingCodeMap(elem))
    }
    codemaps
  }
}