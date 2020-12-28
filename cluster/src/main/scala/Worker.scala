import LinkCheckerApp.args
import akka.actor.{Actor, ActorIdentity, ActorSystem, Identify, Props, RootActorPath, Terminated}
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
  //cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])
  cluster.subscribe(self, classOf[ClusterEvent.MemberUp])
  val main = cluster.selfAddress.copy(port = Some(2552))
  cluster.join(main)

  def receive = {
//    case ClusterEvent.MemberRemoved(m, _) =>
//      if (m.address == main) context.stop(self)
    case ClusterEvent.MemberUp(m) =>
      val act = context.actorSelection(RootActorPath(main) / "user" / "backend" / "receptionist")
      if (m.address == main) act ! Identify("42")

    case ActorIdentity("42", None) => context.stop(self)
    case ActorIdentity("42", Some(ref)) => context.watch(ref)
    case Terminated(_) => context.stop(self)

  }

  override def postStop(): Unit = {
    AsyncWebClient.shutdown()
  }
}
