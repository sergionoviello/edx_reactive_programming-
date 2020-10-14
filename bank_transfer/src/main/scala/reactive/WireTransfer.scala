/*
* this actor is responsible to transfer money from A to B
* it withdraws from A and then it deposit to B
* */
import akka.actor.Actor
import akka.actor.ActorRef

object WireTransfer {
  case class Transfer(from: ActorRef, to: ActorRef, amount: BigInt)
  case object Done
  case object Failed
}

class WireTransfer extends Actor {
  import WireTransfer._

  def receive = {
    case Transfer(from, to, amount) =>
      from ! BankAccount.Withdraw(amount)
      context.become(awaitFrom(to,amount,sender()))
  }

  def awaitFrom(to: ActorRef, amount: BigInt, customer: ActorRef): Receive = {
    case BankAccount.Done =>
      to ! BankAccount.Deposit(amount)
      context.become(awaitTo(customer))

    case BankAccount.Failed =>
      customer ! WireTransfer.Failed
      context.stop(self)
  }

  def awaitTo(customer: ActorRef): Receive = {
    case BankAccount.Done =>
      customer ! Done
      context.stop(self)
  }
}