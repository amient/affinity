package io.amient.akkahttp

import akka.actor.{ActorPath, ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.routing.{ActorRefRoutee, AddRoutee}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.amient.akkahttp.actor.{Gateway, Partition}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object SymmetricClusterNode extends App {

  final val ActorSystemName = "MySymmetricCluster"

  val akkaPort = args(0).toInt
  val host = args(1)
  val httpPort = args(2).toInt
  val numPartitions = args(3).toInt
  val partitionList = args(4).split("\\,").map(_.toInt).toList
  val zkConnect = if (args.length > 5) args(5) else "localhost:2181"
  val zkSessionTimeout = 6000
  val zkConnectTimeout = 30000
  val zkRoot = "/akka"


  println(s"Http port: $httpPort")
  println(s"Akka port: $akkaPort")
  println(s"Partitions: $partitionList of $numPartitions")
  println(s"Zookeeper: $zkConnect")


  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
    .withFallback(ConfigFactory.load("application"))

  implicit val system = ActorSystem(ActorSystemName, config)

  import system.dispatcher

  val log = Logging.getLogger(system, this)

  //construct gateway actor
  val gateway = system.actorOf(Props(new Gateway(numPartitions)), name ="gateway")

  //construct partition actors
  val partitions = for (p <- partitionList) yield {
    system.actorOf(Props(new Partition(p)), name = "partition-" + p)
  }

  val coordinator = new Coordinator(zkRoot, zkConnect, zkSessionTimeout, zkConnectTimeout)


  //register partitions that have booted successfully
  for(p <- partitions) {
    val anchor = ActorPath.fromString(s"akka.tcp://${ActorSystemName}@${host}:${akkaPort}${p.path.toStringWithoutAddress}")
    implicit val timeout =  Timeout(24 hours)
    for(partitionActorRef <- system.actorSelection(anchor).resolveOne()) {
      //gateway ! AddRoutee(ActorRefRoutee(partitionActorRef))
      coordinator.addAnchor(anchor)
      coordinator.subscribeToPartitions(system, gateway) //TODO move this outside the for comprehension
    }
  }


  implicit val materializer = ActorMaterializer.create(system)

  val requestHandler: HttpRequest => Future[HttpResponse] = { httpRequest =>
    implicit val timeout = Timeout(120 seconds)

    val result = for (result <- gateway ? httpRequest) yield result match {
      case response: HttpResponse => response
      case any =>
        log.error("Gateway gave invalid response, expecting HttpResponse, got " + any)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> That's embarrassing..</h1>"))
    }

    result recover {
      case e: Throwable =>
        log.error("Gateway leaked exception", e)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> That's embarrassing..</h1>"))
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
    coordinator.close()
  }
}

