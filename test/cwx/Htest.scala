package cwx

import akka.actor.{Props, ActorSystem}
import dcp.business.actors.HdfsActor


object Htest extends App {

  implicit val system = ActorSystem()
  val hdfsActor = system.actorOf(Props[HdfsActor], "hdfsActor")
}
