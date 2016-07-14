package dgl

import scala.collection.mutable.ArrayBuffer

case class RR(f: String, buf: ArrayBuffer[String])

object TestFormattingTool {
  def main(args: Array[String]) {
    //    val js = LocalFileTool.mkStr("/home/lele/track")
    //    val cleanM = new CleanMachine()
    //    val codemapsdata = CodeMapApp.loadCodeMaps()
    //    val cleanedTrack = cleanM.clean(js.get, codemapsdata)
    //
    //    val hh = TracksFormattingTool.formattingTrack4Hbase(cleanedTrack.get)
    //    println(hh)
    //
    //    val s = TracksFormattingTool.formattingTrackItemScore(cleanedTrack.get)
    //    val t = TracksFormattingTool.formattingTrackTagsScore(cleanedTrack.get)
    //    println("item" + s.size)
    //    s foreach { elem =>
    //      println(elem)
    //    }
    //
    //    println("tags" + t.size)
    //    t foreach { elem =>
    //      println(elem)
    //    }
    //
    //    val esj = TracksFormattingTool.formattingTrack4esj(cleanedTrack.get).get
    //    println(compact(render(esj)))
  }
}
