
import akka.actor.{Actor, ActorSystem, Props, ReceiveTimeout}
import akka.cluster.{Cluster, ClusterEvent}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

object LinkCheckerApp extends App {
  val port = if (args.isEmpty) "0" else args(0)
  val config = ConfigFactory
    .parseString(s"akka.remote.classic.netty.tcp.port=$port")
    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [backend]"))
    .withFallback(ConfigFactory.load())

  val system = ActorSystem("ClusterSystem", config)
  system.actorOf(Props[ClusterMain](), name = "backend")

}

object ClusterMain {

  case object Start

  def props = Props[ClusterMain]

}

class ClusterMain extends Actor {
  import ClusterReceptionist._
  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberUp])
  cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])

  cluster.join(cluster.selfAddress)

  val receptionist = context.actorOf(Props[ClusterReceptionist], "receptionist")
  context.watch(receptionist)


  def getLater(d: FiniteDuration, url: String)= {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(d, receptionist, Get(url))
  }

  getLater(Duration.Zero, "http://www.google.com")


  def receive = {
    case ClusterEvent.MemberUp(member) =>
      if (member.address != cluster.selfAddress) {
        getLater(1.seconds, "http://www.google.com")
        getLater(2.seconds, "http://www.google.com/0")
        getLater(2.seconds, "http://www.google.com/1")
        getLater(3.seconds, "http://www.google.com/2")
        getLater(4.seconds, "http://www.google.com/3")

        context.setReceiveTimeout(3.seconds)
      }

    case Result(url, set)=>
      println(set.toVector.sorted.mkString(s"Results for $url :\n", "\n", "\n"))
    case Failed(url, msg) =>
      println(s"Failed: $url $msg")
    case ReceiveTimeout =>
      cluster.leave(cluster.selfAddress)
    case ClusterEvent.MemberRemoved(m, _) =>
      context.stop(self)
  }
}
