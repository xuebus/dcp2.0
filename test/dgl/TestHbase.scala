package dgl

/**
  * Created by lele on 16-1-13.
  */

import dcp.business.db.HbaseDB
import hapi.Geter.{HBGeter, HBQualifierGeter}
import hapi.Puter.{HBRowPuter, HBFamilyPuter}
import hapi._
import hapi.HBColumn.{HBRowCell, HBColumnFamily, HBColumnRow}
import scala.collection.mutable.ArrayBuffer

object TestHbase {
  private def createDB() = {
    HbaseTool.apply("./DynConfig/HCluster.conf")
    val admin = HbaseTool.getAdmin

    val hf = new HFamily("diu1")
    hf.setMaxVersions(100)

    if (!HManager.tableExist(admin, "dtestapi1111")) {
      val status = HManager.createTable(admin, "dtestapi1111", hf)
      if (!status) println(HBError.getAndCleanErrors())
      else println("创建表成功")
    }

    HbaseTool.closeConnect()
    println("========")
  }

  private def readOne() = {
    /* 读一个 */
    HbaseTool.apply("./DynConfig/HCluster.conf")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    val reader = new HReader()
    val keys = ArrayBuffer[String]()
    for (i <- 1 to 3000) {
      keys += i.toString
    }

    geter.maxVersion = 2
    println("start")
    val s = System.currentTimeMillis
    val hh = reader.getRows(geter, keys)
    val e = System.currentTimeMillis
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResults(rs)
        val ret = reader.resolveResults2Json(rs)
        println(ret)
    }
    println(e - s + " ms")
    println((e - s) / 1000 + " s")
    HbaseTool.closeConnect()
  }

  private def TestHbaseDB() = {
    HbaseTool.apply("./DynConfig/HCluster.conf")
    val admin = HbaseTool.getAdmin
    HbaseDB.createHbaseDB(admin, force = true)
    HbaseTool.closeConnect()
  }

  def main(args: Array[String]) {
//    this.createDB()
    this.TestHbaseDB()
  }
}
