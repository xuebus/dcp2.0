/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供字符串切词功能．
 * 由于无法加载library/ambiguity.dic 文件，会报警告，但不影响系统使用
 * 这个警告可能是库本身的bug
 */

package dcp.business.service

/**
  * 要使更改过的用户字典生效，必须重启应用
  */

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.domain.Term
import org.ansj.library.UserDefineLibrary
import org.ansj.util.MyStaticValue

object AnsjService {
  MyStaticValue.userLibrary = this.userDic
  val status = loadUserLib(MyStaticValue.userLibrary)

  private def userDic  = {
    "DynConfig/user_dictionary.dic"
  }

  private def loadUserLib(dic: String) = {
    try {
      UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, dic)
      true
    } catch {
      case ex: Exception => false
    }
  }

  def strParsing(str: String): Option[java.util.List[Term]] = {
    try {
      Some(ToAnalysis.parse(str))
    } catch {
      case ex: Exception => None
    }
  }
}

/**
  * 提供词性过滤
  */
object AnsjNatureService {
  private val natures = this.loadNatures

  /* toDo 合并后改为加载动态配置 */
  private def loadNatures: Array[String] = {
    Array("m", "ef", "ad", "gd", "n")
  }

  def containsNature(nature: String) = {
    this.natures.contains(nature)
  }
}