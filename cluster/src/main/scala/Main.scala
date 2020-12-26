import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.{Cluster, ClusterEvent}
import com.typesafe.config.ConfigFactory



object LinkCheckerApp extends App {

//  val system = ActorSystem("MainCluster", ConfigFactory.load("node1.conf"))
//  //val system2 = ActorSystem("WorkerCluster", ConfigFactory.load("node2.conf"))
//
//  system.actorOf(ClusterMain.props, "clusterMain")
//
//  //master ! ClusterMain.Start


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

  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberUp])
  cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])

  cluster.join(cluster.selfAddress)

  def receive = {
    case ClusterEvent.MemberUp(member) =>
      println(s"@@@@@@@@@@ ${member.address} ${cluster.selfAddress}")

    case ClusterEvent.MemberRemoved(m, _) =>
      context.stop(self)
  }
}
