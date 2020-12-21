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

  case class Check(sender: ActorRef, id: Long)

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
  var replicatorAcks = Map.empty[Long, (String, Option[ActorRef], Int)]
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

      replicators.foreach(_ ! Replicate(k, Some(v), id))

      if(replicators.nonEmpty) {
        replicatorAcks += ((id, (k, Some(sender), replicators.size)))
      }

      persister ! Persist(k, Some(v), id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! Retry(k, Some(v), id)
      }

      persistenceAcks += id -> sender
      val sn = sender()
      context.system.scheduler.scheduleOnce(1 second) {
        self ! Check(sn, id)
      }
    case Check(sender, id) if ((persistenceAcks get id nonEmpty) || (replicatorAcks get id nonEmpty)) =>
      sender ! OperationFailed(id)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Remove(key, id) =>
      kv = kv.filter(item => item._1 != key)
      sender() ! OperationAck(id)
      replicators.foreach(_ ! Replicate(key, None, id))

    case Replicas(replicas) =>
      replicas.foreach { rep =>
        if (rep != self) {
          val repActor = context.actorOf(Replicator.props(rep))

          kv.foreach { item =>
            val nextId = scala.util.Random.nextInt()
            repActor ! Replicate(item._1, Some(item._2), nextId)

            val newAck:(Long, (String, Option[ActorRef], Int)) = replicatorAcks get nextId match {
              case Some((key, req, missingAcks)) =>
                 (nextId, (key,req, missingAcks+1))
              case None if persistenceAcks get nextId nonEmpty =>
                (nextId, (item._1, Some(persistenceAcks(nextId)), 1))
              case None if persistenceAcks get nextId isEmpty =>
                (nextId, (item._1, None, 1))
            }

            replicatorAcks += newAck
          }

          replicators += repActor
        }
      }
    case Retry(key, v, id) =>
      persister ! Persist(key, v, id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! Retry(key, v, id)
      }
    case Persisted(_, id) if (replicatorAcks get id isEmpty)  =>
      //println(s"Persisted replicatorAcks isEmpty $persistenceAcks $replicatorAcks")

      if (persistenceAcks get id nonEmpty) {
        val req = persistenceAcks(id)
        req ! OperationAck(id)
        persistenceAcks -= id
      }
    case Persisted(_, id) if (replicatorAcks get id nonEmpty) =>
      //println(s"Persisted replicatorAcks nonEmpty $persistenceAcks $replicatorAcks")
      if (persistenceAcks get id nonEmpty) {
        persistenceAcks -= id
      }

    case Replicated(_, id) if (persistenceAcks get id isEmpty) =>
      replicatorAcks(id) match {
        case (key, Some(req), 1) =>
          req ! OperationAck(id)
          replicatorAcks -= id

        case (key, req, acksLeft) =>
          replicatorAcks += ((id, (key, req, acksLeft - 1)))
      }
    case Replicated(k, id) if (persistenceAcks get id nonEmpty) =>
      replicatorAcks(id) match {
        case (key, req, 1) => replicatorAcks -= id
        case (key, req, acksLeft) => replicatorAcks += ((id, (key, req, acksLeft - 1)))

      }

    case _ => //it receives a snapshot message from replicator
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)
    case Snapshot(key, Some(v), seq) if (expectedSeq == seq) => {
      println("snapshot1")
      kv = kv + (key -> v)
      expectedSeq += 1
      persister ! Persist(key, Some(v), seq)

      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! Retry(key, Some(v), seq)
      }

      persistenceAcks += seq -> sender
    }
    case Snapshot(key, None, seq) if (expectedSeq == seq) => {
      println("snapshot2")
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

