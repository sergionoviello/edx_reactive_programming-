import java.util.concurrent.Executor

import org.asynchttpclient.{AsyncHttpClient, Dsl, ListenableFuture, Response}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

trait WebClient {
  def get(url: String)(implicit exec: Executor): Future[String]
}

case class BadStatus(code: Int) extends RuntimeException

object AsyncWebClient extends WebClient {

  private val client = Dsl.asyncHttpClient()

  def getResponse(f: ListenableFuture[Response]): Try[Response] = {
    Try(f.get)
  }

  def get(url: String)(implicit exec: Executor): Future[String] = {
    val f = client.prepareGet(url).execute()
    val p = Promise[String]()
    f.addListener(new Runnable {
      def run() = {

        getResponse(f) match {
          case Success(res) =>
            if (res.getStatusCode < 400) p.success(res.getResponseBody()) //131072
            else p.failure(BadStatus(res.getStatusCode))
          case Failure(e) => println("ERRROR")

        }
      }
    }, exec)
    p.future
  }

  def shutdown(): Unit = {
    client.close()
  }

}
