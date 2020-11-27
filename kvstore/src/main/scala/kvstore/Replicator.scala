package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import kvstore.Arbiter.{Join, JoinedSecondary}
import kvstore.Replica.{Get, Insert, Remove}

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class Retry(key: String, valueOption: Option[String], seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, value, id) =>
      val seq = nextSeq()
      acks += (id -> (sender(), Replicate(key, value, id)))
      println(s"replicate ${replica}")
      replica ! Snapshot(key, value, seq)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! Retry(key, value, seq)
      }

    case Retry(key, value, seq) =>
      replica ! Snapshot(key, value, seq)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! Retry(key, value, seq)
      }

    case SnapshotAck(key, seq) =>
      println("SNAPS")
      acks(seq)._1 ! Replicated(key, seq)
    case _ =>
  }

}
