import akka.actor.{Actor, ActorRef, Address, Deploy, Props, ReceiveTimeout, SupervisorStrategy, Terminated}
import akka.remote.RemoteScope
import scala.concurrent.duration.DurationInt
import ClusterReceptionist._

class Customer(client: ActorRef, url: String, node: Address) extends Actor {

  implicit val s  = context.parent

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  val props = Props[Controller].withDeploy(Deploy(scope = RemoteScope(node)))

  val controller = context.actorOf(props, "controller")

  context.watch(controller)

  context.setReceiveTimeout(5.seconds)
  controller ! Controller.Check(url, 2)

  def receive = ({
    case ReceiveTimeout =>
      context.unwatch(controller)
    case Terminated(_) =>
      client ! ClusterReceptionist.Failed(url, "controller died")
    case Controller.Result(links) =>
      context.unwatch(controller)
      client ! ClusterReceptionist.Result(url, links)
  }:Receive) andThen(_ => context.stop(self))
}
