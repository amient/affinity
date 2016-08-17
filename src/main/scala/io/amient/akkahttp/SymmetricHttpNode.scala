package io.amient.akkahttp

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Identify, Props}
import akka.cluster.Cluster
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.remote.ContainerFormats.ActorIdentity
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.amient.akkahttp.actors.UserInputMediator

import scala.collection.immutable.Iterable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal


/**
  * Created by mharis on 12/08/2016.
  */
object SymmetricHttpNode extends App {

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

  val paths = for (p <- 1 to numPartitions) yield s"akka://$ActorSystemName/user/shard-partition-$p"

  for (p <- partitions) yield {
    system.actorOf(Props(new Shard(p)), name = "shard-partition-" + p)
  }

  Cluster(system) registerOnMemberUp {

    val proxy = system.actorOf(Props(new Proxy(paths)))

    implicit val materializer = ActorMaterializer.create(system)

    val bindingFuture = Http().bindAndHandle(Proxy.route(proxy), host, httpPort)

    println(s"Akka Http Server online at http://$host:$httpPort/\nPress ^C to stop...")

    val binding: ServerBinding = Await.result(bindingFuture, 10 seconds)
    sys.addShutdownHook {
      println("unbinding server port ...")
      Await.result(binding.unbind(), 10 seconds)
      println("server unbound, terminating actor system")
      system.terminate()
    }

  }


}

object Proxy {
  private val internalErrorHandler = ExceptionHandler {
    case NonFatal(e) => complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> Something went wrong ..</h1>"))
    case _ => complete(HttpResponse(
      status = StatusCodes.InternalServerError,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> It's really bad ..</h1>")))
  }

  def route(proxy: ActorRef)(implicit fm: ActorMaterializer): Route = handleExceptions(internalErrorHandler) {

    path("hello") {
      get {
        implicit val timeout = Timeout(60 seconds)
        onSuccess(proxy ? "hello") {
          case line => complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + line + "</h1>"))
        }
      }
    } ~ pathPrefix("hello" / Remaining) { x =>
      implicit val timeout = Timeout(60 seconds)
      onSuccess(proxy ? ("hello", x)) {
        case line => complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + line + "</h1>"))
      }

    } ~ path("hi") {
      get {
        val found = "/hello"
        val locationHeader = headers.Location(found)
        val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
          "<html><body><h2>Please wait..</h2>" +
            "<script>setTimeout(\"location.href = '" + found + "';\",100);</script></body></html>")
        complete(HttpResponse(headers = List(locationHeader), entity = entity))
      } ~ post {
        complete(StatusCodes.MethodNotAllowed)
      }
    }
  }
}

class Proxy(shardPaths: Iterable[String]) extends Actor {

  val roundRobinRouter = context.actorOf(RoundRobinGroup(shardPaths).props(), "RobinRouter")

  val uuid = UUID.randomUUID()

//  for (path <- shardPaths) {
//    implicit val timeout = Timeout(10 seconds)
//    System.err.println(path)
//    context.actorSelection(path).tell(new Identify(uuid), self)
//  }

  context.actorSelection("akka.tcp://MySymmetricCluster@127.0.0.1:2551/user/shard-*")
    .tell(new Identify(uuid), self)
  context.actorSelection("akka.tcp://MySymmetricCluster@127.0.0.1:2552/user/shard-*")
    .tell(new Identify(uuid), self)


  //  val router = {
  //    val routees = for(shardPath <- shardPaths) yield {
  //      println(shardPath)
  //      ActorSelectionRoutee(context.actorSelection(shardPath))
  //    }
  //    Router(RoundRobinRoutingLogic(), routees)
  //  }

  val partitioner: ConsistentHashMapping = {
    case Entry(key, _) => key
    case s: String => s
  }
  //TODO this doesn't ensure consistent hashing aligned with storage partitioning
  val consistentHashRouter = context.actorOf(ConsistentHashingGroup(shardPaths, hashMapping = partitioner).props(), "HashRouter")


  def receive = {
    case id: akka.actor.ActorIdentity => if (id.correlationId == uuid) {
        System.err.println(id.getRef)
    }
    case "hello" =>
      println("simple hello")
      implicit val timeout = Timeout(60 seconds)
      roundRobinRouter ? "hello"
    case ("hello", something) =>
      println("keyed hello: " + something)
      implicit val timeout = Timeout(5 seconds)
      consistentHashRouter ? (something, "hello")
    // TODO watch remote actors and react to termination etc
    //    case Terminated(a) =>
    //      router = router.removeRoutee(a)
    //      val r = context.actorOf(Props[Worker])
    //      context watch r
    //      router = router.addRoutee(r)
  }

}


case class Entry(key: String, value: String)

class Shard(partition: Int) extends Actor {

  val id = self.path.toString

  var cache = Map.empty[String, String]

  val inputMediator = context.actorOf(Props(new UserInputMediator))

  println("Shard: " + self.path);

  def receive = {
    case "hello" =>
      implicit val timeout = Timeout(60 seconds)
      inputMediator ? "hello"

    case Entry(key, value) =>
      println(s"Shard received: ($key, $value)")
      cache += (key -> value)

    case key: String =>
      sender ! cache.get(key)
  }
}

