/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序定义了状态机常用状态的状态模板和数据模板
 */

package dcp.common

import scala.collection.mutable.ArrayBuffer

/* FSM 状态 */
trait FSMState
final case class FSMInit()  //fsm的父actor发送给fsm的初始化消息
case object FSMStart extends FSMState
case object FSMRunning extends FSMState
case object FSMFill extends FSMState

/* FSM 信息事件数据流 */
trait FSMData
case object FSMUninitialized extends FSMData  //没有初始化内部数据时的数据状态
class FSMBuffer[A](buffer: ArrayBuffer[A]) extends FSMData  //内部数据类型
class FSMContent[A](content: A) //消息类型, content消息内容

final case class FSMFlush() //刷新消息