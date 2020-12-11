package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor }
import kvstore.Arbiter._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import scala.language.postfixOps

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

  val persister = context.actorOf(persistenceProps, "persister")
  var persistenceAcks = Map.empty[Long, ActorRef]
  var expectedSeq = 0L

  override def preStart(): Unit = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   =>
      context.become(leader)
    case JoinedSecondary =>
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

    case _ => println("leader not handled")
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)
    case Snapshot(key, Some(v), seq) if (expectedSeq == seq) => {
      kv = kv + (key -> v)
      expectedSeq += 1
      persister ! Persist(key, Some(v), seq)

      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! Retry(key, Some(v), seq)
      }

      persistenceAcks += seq -> sender
    }
    case Snapshot(key, None, seq) if (expectedSeq == seq) => {
      kv -= key
      expectedSeq += 1
      persister ! Persist(key, None, seq)

      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! Retry(key, None, seq)
      }
      persistenceAcks += seq -> sender
    }

    case Retry(key, v, id) =>
      persister ! Persist(key, v, id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! Retry(key, v, id)
      }
    case Persisted(key, id) =>
      val req = persistenceAcks(id)

      req ! SnapshotAck(key, id)
      persistenceAcks -= id
    case _ =>  println("replica not handled")
  }

}

