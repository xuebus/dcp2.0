package dgl

import akka.actor.{Props, ActorSystem}
import dcp.business.actors.FileCleanActor

object TestFileClean {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    val cleanActor = system.actorOf(Props[FileCleanActor], "CleanActor")
    println(cleanActor.path)
    Thread.sleep(5000)
    system.shutdown()
  }
}
