package io.amient.akkahttp

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.amient.akkahttp.actors.{Gateway, Partition}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

object SymmetricNode extends App {

  final val ActorSystemName = "MySymmetricCluster"

  val akkaPort = args(0).toInt
  val host = args(1)
  val httpPort = args(2).toInt
  val numPartitions = args(3).toInt
  val partitions = args(4).split("\\,").map(_.toInt).toList


  println(s"Http port: $httpPort")
  println(s"Akka port: $akkaPort")
  println(s"Partitions: $partitions of $numPartitions")

  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
    .withFallback(ConfigFactory.load("application"))

  implicit val system = ActorSystem(ActorSystemName, config)

  import system.dispatcher

  val gateway = system.actorOf(Props(new Gateway))

  for (p <- partitions) yield {
    system.actorOf(Props(new Partition(p)), name = "shard-partition-" + p)
  }

  implicit val materializer = ActorMaterializer.create(system)

  val requestHandler: HttpRequest => Future[HttpResponse] = { httpRequest =>
    implicit val timeout = Timeout(120 seconds)
    (for (result <- gateway ? httpRequest) yield result match {
      case response: HttpResponse => response
      case _ =>
        sys.error("Can't do that!")
    }) recover {
      case e: IllegalArgumentException => HttpResponse(status = StatusCodes.BadRequest,
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> Bad Request..</h1>"))
      case NonFatal(e) => HttpResponse(status = StatusCodes.InternalServerError,
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> It's bad but we should be back..</h1>"))
      case e: Throwable => HttpResponse(status = StatusCodes.InternalServerError,
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> It's really bad ..</h1>"))
    }
  }
  val bindingFuture = Http().bindAndHandleAsync(requestHandler, host, httpPort)

  bindingFuture.onFailure {
    //TODO server cannot bind to the port
    case e: Exception => e.printStackTrace()
  }
  println(s"Akka Http Server online at http://$host:$httpPort/\nPress ^C to stop...")

  val binding: ServerBinding = Await.result(bindingFuture, 10 seconds)

  sys.addShutdownHook {
    println("unbinding server port ...")
    Await.result(binding.unbind(), 10 seconds)
    println("server unbound, terminating actor system")
    system.terminate()
  }
}

