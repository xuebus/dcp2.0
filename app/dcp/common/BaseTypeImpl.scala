/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供一些基本类型到其他类型的隐式转换
 */

package dcp.common

object BaseTypeImpl {
  import scala.language.implicitConversions

  implicit def longToInt(value: Long): Int = value.toInt
  implicit def stringToBoolean(value: String): Boolean = {
    value match {
      case "false" => false
      case "true" => true
      case _ => throw new Exception(s"None, value $value can't change to Boolean")
    }
  }
}
