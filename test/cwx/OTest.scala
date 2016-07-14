package cwx

import BoDaoCommon.File.LocalFileTool
import akka.actor.{ActorSystem, Props}
import dcp.business.actors.OryxActor
import dcp.business.codemap.CodeMapApp
import dcp.business.service.CleanMachine
import dcp.common.CleanedTrack


object OTest extends App {
  implicit val system = ActorSystem()
  val oryxActor = system.actorOf(Props[OryxActor], "oryxActor")

  val js = LocalFileTool.mkStr("./track")
      val cleanM = new CleanMachine()
      val codemapsdata = CodeMapApp.loadCodeMaps()
      val cleanedTrack = cleanM.clean(js.get, codemapsdata)

  for (i <- 1 to 10) {
    Thread.sleep(500)
    oryxActor ! CleanedTrack(cleanedTrack.get)
  }

}
