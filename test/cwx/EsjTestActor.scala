package cwx

import akka.actor.Actor
import cwx.EsjTestActor.EsjTestInit
import dcp.common.EsjTracks

import scala.collection.mutable.ArrayBuffer

class EsjTestActor extends Actor {

  val numS = 1
  var tracksToSend = ArrayBuffer[String]()
  val qName = "rawdata1_BOdao2015*"

  def sendData(num: Int, name: String ,tracks: Option[ArrayBuffer[String]]) = {
    for (i <- 1 to num) {
      val fqueueClientActor = context.actorSelection(s"/user/fqueueActor/fqueueSendActor$i")
      val tracksData = tracks.get
      fqueueClientActor ! EsjTracks(name, tracksData)
    }
  }

  def receive = {
    case EsjTestInit =>
      for (i <- 1 to 100) {
      tracksToSend += s"$i"
    }
      sendData(numS, qName, Some(tracksToSend))
  }

}

object EsjTestActor {
  case class EsjTestInit()
}
