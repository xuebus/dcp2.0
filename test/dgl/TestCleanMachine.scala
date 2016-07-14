package dgl

import dcp.business.codemap.CodeMapApp
import dcp.business.service.{AnsjService, CleanMachine}
import BoDaoCommon.File.LocalFileTool
import org.ansj.domain.Term

import scala.collection.mutable
import math._
import org.ansj.recognition.NatureRecognition

object TestCleanMachine {
  def main(args: Array[String]) {
    val js = LocalFileTool.mkStr("/home/lele/track")
    val cleanM = new CleanMachine()
    val codemapsdata = CodeMapApp.loadCodeMaps()
    cleanM.clean(js.get, codemapsdata)
//    val str = "一支淡化细纹、紧肤无敌的护肤品的乳清蛋白"
//    val ret = AnsjService.strParsing(str).get
//    ret.toArray() foreach { elem =>
//      println(elem.toString)
//      println(elem.asInstanceOf[Term].getNatureStr)
//      if (elem.asInstanceOf[Term].getNatureStr == "m") println("^^^^^")
//    }
//
//    val hh = AnsjService.strParsing(str)
//    println(hh)
  }
}