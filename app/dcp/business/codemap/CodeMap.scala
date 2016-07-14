/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序将codemap的几种类型抽象为多个类,用来加载到内存供dcp actors使用
 * CodeMap　提供一个模板实现
 * TraversableCodeMap 是codemap的值为list类型的抽象，如page, item, cate, itemInfo
 * StringCodeMap 是codemap的值为string类型的抽象,如tags
 */

package dcp.business.codemap

import BoDaoCommon.File.LocalFileTool

/**
  * 提供codemap模板
  */
sealed trait CodeMap {
  protected def loadCodeMaps(file: String): Map[String, String] = {
    val codemapOpt = LocalFileTool.readFile2kv(file, "\t")
    codemapOpt match {
      case None => Map[String, String]()
      case Some(codemapData) => codemapData.toMap
    }
  }

  def getCode(key: String): Option[Any]

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def size: Int
}

/**
  * map 元素可以做成list形式的，使用这个类
  */
final class TraversableCodeMap(defaultFile: String, dtCodeMapFile:String, name: String) extends CodeMap {
  private val _codemap = this.load()

  /**
    * 做一个隐式转换，将string转换为TraversableOnce[String]
    */
  import scala.language.implicitConversions
  protected implicit def stringToArray(data: Map[String, String]): Map[String, TraversableOnce[String]] = {
    data mapValues { value =>
      value.split(",").map(v => v.trim) //去掉空格，tab等
    }
  }

  private def load(): Map[String, TraversableOnce[String]] = {
    val defCodeMap = loadCodeMaps(defaultFile)
    val dtCodeMap = loadCodeMaps(dtCodeMapFile) //增量的codemaps
    defCodeMap ++ dtCodeMap
  }

  def getCode(key: String): Option[TraversableOnce[String]] = this._codemap.par.get(key)

  def isEmpty = this._codemap.isEmpty
  def nonEmpty = this._codemap.nonEmpty
  def size = this._codemap.size
  def codemap = this._codemap //用于测试
}

/**
  * map 元素可以做成string形式的，使用这个类
  */
final class StringCodeMap(defaultFile: String, dtCodeMapFile:String, name: String) extends CodeMap {
  private val _codemap = this.load()

  private def load(): Map[String, String] = {
    val defCodeMap = loadCodeMaps(defaultFile)
    val dtCodeMap = loadCodeMaps(dtCodeMapFile) //增量的codemaps
    defCodeMap ++ dtCodeMap
  }

  def getCode(key: String): Option[String] = this._codemap.par.get(key)

  def isEmpty = this._codemap.isEmpty
  def nonEmpty = this._codemap.nonEmpty
  def size = this._codemap.size
  def codemap = this._codemap
}