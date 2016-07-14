/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ cwx
 *
 *  本程序用來模擬FqueueToolActor來對fqueueClientActor取數據
 *
 *
 *
 */
package cwx

import akka.actor.Actor
import cwx.FqueueToolTestActor.FqueueToolTestInit
import dcp.common.{FqueueData, FqueueName}

class FqueueToolTestActor extends Actor {

  val qName = "codemap_BOdao2015*"
  val cNum = 6
  var count = 0

  def requireData(num: Int, name: String): Unit = {
    for (i <- 1 to num) {
      val fqueueClientActor = context.actorSelection(s"/user/fqueueActor/fqueueObtainActor$i")
      fqueueClientActor ! FqueueName(name)
    }
  }

  def receive = {
    case FqueueToolTestInit =>
      requireData(cNum, qName)
    case FqueueData(data) =>
      data match {
        case Some(str) => count += 1
        case None => println(count)
      }
    case _ =>
  }

}

object FqueueToolTestActor {
  case class FqueueToolTestInit()
}
