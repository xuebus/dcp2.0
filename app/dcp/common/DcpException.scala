/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供一些基本异常类型, 每个模块一个
 * 现在这些异常类型作用并不大, 是为日后扩展准备的
 */

package dcp.common

case class DcpException() extends Exception()
case class HbaseException() extends Exception()
case class EsjException() extends Exception()
case class OryxException() extends Exception()
case class HdfsException() extends Exception()
case class FqueueException() extends Exception()
case class CodeMapException() extends Exception()