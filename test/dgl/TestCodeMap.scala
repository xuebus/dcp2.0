package dgl

/**
  * Created by lele on 15-12-31.
  */
import dcp.business.codemap.{StringCodeMap, TraversableCodeMap, CodeMap, CodeMapApp}
import BoDaoCommon.File.LocalFileTool
import dcp.config.ConfigurationEngine

object TestCodeMap {
  def main(args: Array[String]) {
//    val str = LocalFileTool.mkStr("/home/lele/codemap").get
//    val ret = CodeMapApp.parsingCodeMaps(str)
//    ret.keys foreach { cmtOpt => {
//      ret(cmtOpt) match {
//        case None => println("None")
//        case Some(cmt) => println(cmt.values)
//      }
//    }
//    }

//    val root = ConfigurationEngine.getDir("codemap.dir", "./DynConfig/codemap")
//    val defFile = s"$root/default/page"
//    val dtFile = s"$root/page"
//    val codemap = new CodeMap(defFile, dtFile, "page")
//    println(codemap.getCode("rd302236").get)
//    println(codemap.getCodeMap("rd302236").size)
//    val data = codemap.getCodeMap("rd302236")
//    data foreach { d =>
//      println(d)
//    }

    val codemapsdata = CodeMapApp.loadCodeMaps()
//    println(codemapsdata)
    codemapsdata.keys foreach { key =>
      println(key)
      key match {
        case "page" =>
          val data = codemapsdata(key).asInstanceOf[TraversableCodeMap].codemap
          val hh = data.get("rd302236").get
          println(data)
          hh foreach { v =>
            println(s"--$v--")
          }
        case "cate" =>
          val data = codemapsdata(key).asInstanceOf[TraversableCodeMap].codemap
          println(data)
        case "item" =>
          val data = codemapsdata(key).asInstanceOf[TraversableCodeMap].codemap
          println(data)
        case "itemInfo" =>
          val data = codemapsdata(key).asInstanceOf[TraversableCodeMap].codemap
          println(data)
        case "tags" =>
          val data = codemapsdata(key).asInstanceOf[StringCodeMap].codemap
          println(data)
      }
      println("--------")
    }
  }
}
