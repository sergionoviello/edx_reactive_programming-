package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor }
import kvstore.Arbiter._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSeq = 0L

  override def preStart(): Unit = {
    println("EEEEE")
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   =>
      println("prim")
      context.become(leader)
    case JoinedSecondary =>
      println("sec")
      context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) =>
      kv += (k -> v)
      sender() ! OperationAck(id)
      replicators.foreach(_ ! Replicate(k, Some(v), id))
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Remove(key, id) =>
      kv = kv.filter(item => item._1 != key)
      sender() ! OperationAck(id)
      replicators.foreach(_ ! Replicate(key, None, id))

    case Replicas(replicas) =>
      replicas.foreach { rep =>
        val repActor = context.actorOf(Replicator.props(rep))

        kv.foreach(item => repActor ! Replicate(item._1, Some(item._2), scala.util.Random.nextInt()))

        replicators += repActor
      }

    case _ =>
      println("BBBBB")
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      if (expectedSeq == seq) {
        valueOption match {
          case Some(v) =>
            kv = kv + (key -> valueOption.get)
          case None =>
            kv -= key
        }

        expectedSeq += 1
        sender() ! SnapshotAck(key, seq)
      }
    case _ =>
      println("AAAAA")
  }

}

