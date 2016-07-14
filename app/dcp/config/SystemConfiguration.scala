/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供系统配置和系统动态配置
 * 系统配置是为了独立paly框架的配置
 * 系统动态配置是为了用户可以动态加载常改的配置项, 不用再打包一次程序再次部署
 */

package dcp.config

import scala.collection.mutable.{Map => MuMap}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * 系统play2 的配置
  */
object SystemConfiguration {
  private lazy val cfgFileName = "application.conf"
  var _config: Option[Config] = None

  private def load(cfgFile: String) = {
    val cfg = ConfigFactory.load(cfgFile)
    _config = Some(cfg)
    cfg
  }

  def config: Config = {
    _config match {
      case None => load(cfgFileName)
      case Some(cfg) => cfg
    }
  }
}

class DynConfig() {
  import dcp.common.BaseTypeImpl._

  private lazy val _configs = MuMap[String, String]()

  def this(configLines: Map[String, String]) = {
    this()
    this.init(configLines)
  }

  private def init(configLines: Map[String, String]) = {
    configLines.keys foreach { configLineKey =>
      val value = this.removeQuotes(configLines(configLineKey))
      _configs += (configLineKey -> value)
    }
  }

  /**
    * 移除头尾'\"'字符
    * 如果字符为null或者长度小于等于2， 不进行操作
    * 如果只有一边存在 '\"', 不进行操作
    * @param value 要操作的字符
    * @return
    */
  private def removeQuotes(value: String) = {
    if (value != null && value.length > 2) {  //长度小于等于2时，不可能包'\"'
      if (value.charAt(0) == '\"' && value.charAt(value.length - 1) == '\"') {
        value.substring(1, value.length - 1)  //移除头尾'\"'字符
      } else {
        value
      }
    } else {
      value
    }
  }

  def getString(key: String): String = {
    val value = _configs.get(key)
    value match {
      case None => throw new Exception("None")
      case Some(str) => removeQuotes(str)
    }
  }

  def getBoolean(key: String): Boolean = {
    val value = _configs.get(key)
    value match {
      case None => throw new Exception("None")
      case Some(str) => str
    }
  }

  def getInt(key: String): Int = {
    this.getLong(key)
  }

  def getLong(key: String): Long = {
    val value = _configs.get(key)
    value match {
      case None => throw new Exception("None")
      case Some(str) => str.toLong
    }
  }

}

object DynConfigFactory {
  def load(cfgFile: String): DynConfig = {
    import BoDaoCommon.File.LocalFileTool
    val cfgLines = LocalFileTool.readFile2kv(cfgFile, "=")
    cfgLines match {
      case None => throw new Exception(s"read config file $cfgFile exception")
      case Some(cls) => new DynConfig(cls.toMap)
    }
  }
}

object DynConfiguration {
  private var _config: Option[DynConfig] = None
  private lazy val cfgFile = "./DynConfig/DynamicApplication.conf"

  def config: DynConfig = {
    _config match {
      case None =>
        val cfg = DynConfigFactory.load(cfgFile)
        _config = Some(cfg)
        cfg
      case Some(cfg) => cfg
    }
  }

  def getStringOrElse(key: String, default: String): String = {
    try {
      this.config.getString(key)
    } catch {
      case ex: Exception => default
    }
  }

  def getBooleanOrElse(key: String, default: Boolean): Boolean = {
    try {
      this.config.getBoolean(key)
    } catch {
      case ex: Exception => default
    }
  }

  def getIntOrElse(key: String, defaule: Int) = {
    try {
      this.config.getInt(key)
    } catch {
      case ex: Exception => defaule
    }
  }

  def getLongOrElse(key: String, default: Long) = {
    try {
      this.config.getLong(key)
    } catch {
      case ex: Exception => default
    }
  }
}