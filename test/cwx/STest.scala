package cwx

import akka.actor.{Props, ActorSystem}
import dcp.business.actors.{TrafficStatistics, StatisticsActor}

object STest extends App {
  implicit val system = ActorSystem()
  val statisticsActor = system.actorOf(Props[StatisticsActor], "statisticsActor")

  for (i <- 1 to 100) {
    Thread.sleep(500)
    statisticsActor ! TrafficStatistics(i)
  }
}
