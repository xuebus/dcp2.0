package dgl

import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.{ TestKit, ImplicitSender }
import dcp.business.actors.HbaseRouter
import org.specs2.mutable.SpecificationLike

case class GGG(msg: String)
case class TestMsg(msggg: GGG)

class TestDcpActor extends TestKit(ActorSystem()) with ImplicitSender with SpecificationLike {
  "An Echo actor" should {

    "send back messages unchanged" in {
      val echo = system.actorOf(Props[HbaseRouter],"echo-actor")
      val ggg = GGG("lllllll")
      val msg = TestMsg(ggg)
      echo ! msg
      println("waiting for echo reply from echo actor")
      expectMsg(msg)
      success
    }

  }
}