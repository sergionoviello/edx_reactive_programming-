import LinkCheckerApp.args
import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.{Cluster, ClusterEvent}
import com.typesafe.config.ConfigFactory

object Worker extends App {
  val port = if (args.isEmpty) "0" else args(0)

  val config = ConfigFactory
    .parseString(s"""
      akka.remote.classic.netty.tcp.port=$port
      akka.cluster.auto-down=on
    """)
    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]"))
    .withFallback(ConfigFactory.load())

  val system = ActorSystem("ClusterSystem", config)
  system.actorOf(Props[ClusterWorker](), name = "worker")
}


object ClusterWorker {

  case object Start

  def props = Props[ClusterWorker]

}

class ClusterWorker extends Actor {

  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])
  val main = cluster.selfAddress.copy(port = Some(2552))
  cluster.join(main)

  def receive = {
    case ClusterEvent.MemberRemoved(m, _) =>
      if (m.address == main) context.stop(self)
  }

  override def postStop(): Unit = {
    AsyncWebClient.shutdown()
  }
}
