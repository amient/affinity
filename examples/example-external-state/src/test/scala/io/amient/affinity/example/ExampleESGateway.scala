package io.amient.affinity.example

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpMethods.GET
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp, Partition}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.util.Scatter

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleESGateway extends GatewayHttp {

  val keyspace: ActorRef = keyspace("external")

  import context.dispatcher
  implicit val scheduler = context.system.scheduler
  override def handle: Receive = {

    case HTTP(GET, PATH("news", "latest"), _, response) =>
      implicit val timeout = Timeout(3 second)
      val latestNews: Future[List[String]] = keyspace gather GetLatest()
      val responseText = latestNews.map(news=> "LATEST NEWS:\n" + news.mkString("\n"))
      handleAsText(response, responseText)
  }

}


case class GetLatest() extends Scatter[List[String]] {
  override def gather(r1: List[String], r2: List[String]) = r1 ++ r2
}


class ExampleESPartition extends Partition {


  val latest = new ConcurrentLinkedQueue[(String, String)]()

  state[String, String]("news").listen {
    case (k: String, v: String) =>
      latest.add((k, v))
      while (latest.size() > 3) latest.poll()
  }

  override def handle: Receive = {
    case request@GetLatest() => sender.reply(request) {
      latest.iterator.asScala.map(_.productIterator.mkString(("\t"))).toList
    }
  }
}
