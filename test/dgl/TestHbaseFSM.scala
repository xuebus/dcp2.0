package dgl

import BoDaoCommon.File.LocalFileTool
import akka.actor.{ActorSystem, Props}
import dcp.business.actors.HbaseRouter
import dcp.business.codemap.CodeMapApp
import dcp.business.db.HbaseDB
import dcp.business.service.CleanMachine
import dcp.common.CleanedTrack
import hapi.HbaseTool


/**
  * Created by lele on 16-1-18.
  */
object TestHbaseFSM {
  private val optionFilter: (Option[Any], Option[Any] => Boolean) => Boolean =
    (opt: Option[Any], func: Option[Any] => Boolean) => {
      func(opt)
  }

  private def trackInHbaseOptFilter(trackInHbaseOpt: Option[Any]): Boolean = {
    trackInHbaseOpt match {
      case None => false
      case Some(t) => true
    }
  }

  private def pageInfoAndActionFilter(pageInfos: Option[String], actions: Option[String]): Boolean = {
    (for {
      p <- pageInfos
      a <- actions
    } yield a) match {
      case None => false
      case Some(t) => true
    }
  }

  private def initHbaseTool() = {
    HbaseTool.apply("./DynConfig/HCluster.conf")
  }

  private def createTableForce() = {
    HbaseDB.createHbaseDB(HbaseTool.getAdmin, force = true)
  }

  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    val hbaseMessenger = system.actorOf(Props[HbaseRouter], "HbaseActor")
    println("sleep 5 s")
    Thread.sleep(5000)

    val js = LocalFileTool.mkStr("/home/lele/track")
    val cleanM = new CleanMachine()
    val codemapsdata = CodeMapApp.loadCodeMaps()
    val cleanedTrack = cleanM.clean(js.get, codemapsdata).get
    val uid = cleanedTrack.uid

    for (i <- 1 to 2) {
      hbaseMessenger ! CleanedTrack(cleanedTrack.copy(uid = s"$i"))
    }

    println("send finished")
    Thread.sleep(12000)
    println("gbi")
    system.shutdown()
    HbaseTool.closeConnect()
  }
}
