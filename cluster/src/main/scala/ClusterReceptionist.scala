import akka.actor.{Actor, ActorRef, Address, Props}
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.cluster.{Cluster, ClusterEvent}

object ClusterReceptionist {

  case class Job(client: ActorRef, url: String)
  case class Failed(url: String, msg: String)
  case class Get(url: String)
  case class Result(url: String, links: Set[String])

  def props = Props[ClusterReceptionist]

}

class ClusterReceptionist extends Actor {
  import ClusterReceptionist._

  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])
  cluster.subscribe(self, classOf[ClusterEvent.MemberUp])

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def receive = awaitingMembers

  def awaitingMembers: Receive = {
    case current:ClusterEvent.CurrentClusterState =>
      val addresses = current.members.toVector.map(_.address)
      val notMe = addresses.filter(_ != cluster.selfAddress)
      if(notMe.nonEmpty) context.become(active(notMe))
    case MemberUp(m) if m.address != cluster.selfAddress =>
      context.become(active(Vector(m.address)))
    case Get(url) => sender ! Failed(url, "No nodes")
  }


  def active(addresses: Vector[Address]): Receive = {
    case MemberUp(m) if m.address != cluster.selfAddress =>
      context.become(active(addresses :+ m.address ))
    case MemberRemoved(m, _) =>
      val next = addresses filterNot(_ == m.address)
      if (next.isEmpty) context.become(awaitingMembers)
      else context.become(active(next))
    case Get(url) if context.children.size < addresses.size =>
      val client = sender()
      val address = addresses.last
      context.actorOf(Props(new Customer(client, url, address)))
    case Get(url) =>
      sender ! Failed(url, "too many queries")

  }
}
