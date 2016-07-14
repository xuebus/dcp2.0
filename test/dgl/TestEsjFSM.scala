package dgl

import BoDaoCommon.File.LocalFileTool
import akka.actor.{Props, ActorSystem}
import dcp.business.actors.EsjRouter
import dcp.business.codemap.CodeMapApp
import dcp.business.service.CleanMachine
import dcp.common.CleanedTrack

/**
  * Created by lele on 16-1-21.
  */
object TestEsjFSM {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    val esjRouter = system.actorOf(Props[EsjRouter], "HbaseActor")
    Thread.sleep(2000)
    val js = LocalFileTool.mkStr("/home/lele/track")
    val cleanM = new CleanMachine()
    val codemapsdata = CodeMapApp.loadCodeMaps()
    val cleanedTrack = cleanM.clean(js.get, codemapsdata).get
    val uid = cleanedTrack.uid

    for (i <- 1 to 10) {
      esjRouter ! CleanedTrack(cleanedTrack.copy(uid = s"$i"))
    }

    println("send finished")
    Thread.sleep(4500)
    println("gbi")
    system.shutdown()
  }
}
