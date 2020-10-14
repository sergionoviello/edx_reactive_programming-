
import akka.actor.Actor

object BankAccount {
  case class Deposit(amount: BigInt)
  case class Withdraw(amount: BigInt)
  case object Done
  case object Failed
}

class BankAccount extends Actor {
  import BankAccount._

  var balance = BigInt(0)

  def receive = {
    case Deposit(amount) =>
      balance += amount
      sender ! Done
    case Withdraw(amount) if (balance >= amount) =>
      balance -= amount
      sender ! Done

    case _ => sender ! Failed
  }
}