import java.util.concurrent.Executor

import org.asynchttpclient.{AsyncHttpClient, Dsl}

import scala.concurrent.{Future, Promise}

trait WebClient {
  def get(url: String)(implicit exec: Executor): Future[String]
}

case class BadStatus(code: Int) extends RuntimeException

object AsyncWebClient extends WebClient {

  private val client = Dsl.asyncHttpClient()

  def get(url: String)(implicit exec: Executor): Future[String] = {
    val f = client.prepareGet(url).execute()
    val p = Promise[String]()
    f.addListener(new Runnable {
      def run() = {
        val response = f.get
        if (response.getStatusCode < 400)
          p.success(response.getResponseBody()) //131072
        else p.failure(BadStatus(response.getStatusCode))
      }
    }, exec)
    p.future
  }

  def shutdown(): Unit = {
    client.close()
  }

}
