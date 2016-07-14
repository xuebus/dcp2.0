package cwx

import BoDaoCommon.File.LocalFileTool
import akka.actor.{ActorSystem, Props}
import dcp.business.actors.{EsjRouter, FqueueRouter}
import dcp.business.codemap.CodeMapApp
import dcp.business.service.CleanMachine
import dcp.common.CleanedTrack

/**
 * Created by cwx on 16-1-27.
 */
object EandFTest extends App {

  implicit val system = ActorSystem()
  val fqueueRouter = system.actorOf(Props[FqueueRouter], "fqueueRouter")
  val esjRouter = system.actorOf(Props[EsjRouter], "esjRouter")
  Thread.sleep(2000)
  val js = LocalFileTool.mkStr("./track")
  val cleanM = new CleanMachine()
  val codemapsdata = CodeMapApp.loadCodeMaps()
  val cleanedTrack = cleanM.clean(js.get, codemapsdata).get
  val uid = cleanedTrack.uid

  for (i <- 1 to 10) {
    Thread.sleep(1000)
    esjRouter ! CleanedTrack(cleanedTrack.copy(uid = s"$i"))
  }
}
