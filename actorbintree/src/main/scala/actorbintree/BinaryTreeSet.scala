/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._

import scala.collection.immutable.Queue
import scala.util.Random

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case msg =>
      root ! msg
    case GC =>
      val newRoot = root
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))

  }



  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case _:Operation => stash()
    case CopyFinished =>
      root = newRoot
      context.unbecome()
      unstashAll()
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved
  var countNodes = 0

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(req, id, el) =>

      if (el > elem) {
        doInsert(Right, req, id, el)
      }
      else if (el < elem) {
        doInsert(Left, req, id, el)
      } else {
        if (removed) removed = false
        req ! OperationFinished(id)
      }

    case Contains(req, id, el) =>
      if (el > elem) {
        doContains(Right, req, id, el)
      }
      else if (el < elem) {
        doContains(Left, req, id, el)
      } else {
        req ! ContainsResult(id, !removed)
      }

    case Remove(req, id, el) =>
      if (el > elem) {
        doRemove(Right, req, id, el)
      }
      else if (el < elem) {
        doRemove(Left, req, id, el)
      } else {
        removed = true
        req ! OperationFinished(id)
      }

    case CopyTo(newRoot) =>
      if(!removed) newRoot ! Insert(self, Random.nextInt(), elem)
      if(subtrees.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      }
      else {
        subtrees.values.foreach(ref => ref ! CopyTo(ref))
      }
    case CopyFinished =>
      countNodes += 1
      if (countNodes == subtrees.size) {
        context.parent ! CopyFinished
        context.stop(self)
      }
    case _ => //ignore
  }

  def doInsert(pos: Position, req: ActorRef, id: Int,  el: Int) = {
    subtrees.get(pos) match {
      case Some(ref) => //non leaf
        ref ! Insert(req, id, el)
      case None => // leaf
        val child = context.actorOf(BinaryTreeNode.props(el, initiallyRemoved = false))
        subtrees += (pos -> child)

        req ! OperationFinished(id)
    }
  }

  def doContains(pos: Position, req: ActorRef, id: Int,  el: Int)  = {
    subtrees.get(pos) match {
      case Some(ref) => //non leaf
        ref ! Contains(req, id, el)
      case None =>
        req ! ContainsResult(id, false)
    }
  }

  def doRemove(pos: Position, req: ActorRef, id: Int,  el: Int)  = {
    subtrees.get(pos) match {
      case Some(ref) => //non leaf
        ref ! Remove(req, id, el)
      case None =>
        req ! OperationFinished(id)
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
