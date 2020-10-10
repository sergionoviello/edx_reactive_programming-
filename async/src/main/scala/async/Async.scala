package async

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object Async extends AsyncInterface {

  /**
    * Transforms a successful asynchronous `Int` computation
    * into a `Boolean` indicating whether the number was even or not.
    * In case the given `Future` value failed, this method
    * should return a failed `Future` with the same error.
    */
  def transformSuccess(eventuallyX: Future[Int]): Future[Boolean] =
    eventuallyX.flatMap {
      case el => Future.successful(el % 2 == 0)
    }.recoverWith {
      case ex => Future.failed(ex)
    }
//    eventuallyX.onComplete {
//      case Success(el) => Future { el % 2 == 0 }
//      case Failure(ex) => Future.failed(ex)
//    }


  /**
    * Transforms a failed asynchronous `Int` computation into a
    * successful one returning `-1`.
    * Any non-fatal failure should be recovered.
    * In case the given `Future` value was successful, this method
    * should return a successful `Future` with the same value.
    */
  def recoverFailure(eventuallyX: Future[Int]): Future[Int] =
    eventuallyX.flatMap {
      case el => Future.successful(el)
      }.recover {
      case ex => -1
      }

  /**
    * Perform two asynchronous computation, one after the other. `makeAsyncComputation2`
    * should start ''after'' the `Future` returned by `makeAsyncComputation1` has
    * completed.
    * In case the first asynchronous computation failed, the second one should not even
    * be started.
    * The returned `Future` value should contain the successful result of the first and
    * second asynchronous computations, paired together.
    */
  def sequenceComputations[A, B](
    makeAsyncComputation1: () => Future[A],
    makeAsyncComputation2: () => Future[B]
  ): Future[(A, B)] =
    makeAsyncComputation1().flatMap { comp1 =>
      makeAsyncComputation2().flatMap { comp2 =>
        Future.successful((comp1, comp2))
      }
    }

  /**
    * Concurrently perform two asynchronous computations and pair their successful
    * result together.
    * The two computations should be started independently of each other.
    * If one of them fails, this method should return the failure.
    */
  def concurrentComputations[A, B](
    makeAsyncComputation1: () => Future[A],
    makeAsyncComputation2: () => Future[B]
  ): Future[(A, B)] = {
    val operation = makeAsyncComputation1().zip(makeAsyncComputation2())
    operation.flatMap {
      case res => Future.successful{ (res._1, res._2) }
    }
  }

  /**
    * Attempt to perform an asynchronous computation.
    * In case of failure this method should try again to make
    * the asynchronous computation so that at most `maxAttempts`
    * are eventually performed.
    */
  def insist[A](makeAsyncComputation: () => Future[A], maxAttempts: Int): Future[A] =
    makeAsyncComputation().recoverWith { case _ if maxAttempts > 1 => insist(makeAsyncComputation, maxAttempts - 1) }

  /**
    * Turns a callback-based API into a Future-based API
    * @return A `FutureBasedApi` that forwards calls to `computeIntAsync` to the `callbackBasedApi`
    *         and returns its result in a `Future` value
    *
    * Hint: Use a `Promise`
    */
  def futurize(callbackBasedApi: CallbackBasedApi): FutureBasedApi = {
    val p = Promise[Int]()

    callbackBasedApi.computeIntAsync(
      el => {
        el match {
          case Success(e) => p.trySuccess(e)
          case Failure(ex) => p.tryFailure(ex)
        }
      }
    )

    new FutureBasedApi {
      def computeIntAsync = p.future
    }
  }
}

/**
  * Dummy example of a callback-based API
  */
trait CallbackBasedApi {
  def computeIntAsync(continuation: Try[Int] => Unit): Unit
}

/**
  * API similar to [[CallbackBasedApi]], but based on `Future` instead
  */
trait FutureBasedApi {
  def computeIntAsync(): Future[Int]
}
