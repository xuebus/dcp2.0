package dgl

import dcp.config._

object TestConfig {
  def main(args: Array[String]) {
    val b = DynConfiguration.getBooleanOrElse("test.str.false", false)
    val l = DynConfiguration.getIntOrElse("fqueue.connection.timeout", 1)
    val s = DynConfiguration.getStringOrElse("fqueue.queuename.codemap", "")
    println(s"--${b}--")
    println(s"--${l}--")
    println(s"--${s}--")
  }
}
