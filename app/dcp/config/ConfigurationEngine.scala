/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供系某些配置指令的解析引擎
 */

package dcp.config

object ConfigurationEngine {
  val dynCfg = DynConfiguration

  def getDir(cmd: String, default: String): String = {
    dynCfg.getStringOrElse(cmd, default).stripSuffix("/")  //一律将末尾的"/"去掉
  }

  def getTime2Millisecond(cmd: String, default: Int): Long = {
    val second = dynCfg.getIntOrElse(cmd, default)
    if (second > 0) second * 1000 else default * 1000
  }
}