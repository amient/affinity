package io.amient.akkahttp.actors

import java.util.UUID

import akka.actor.{Actor, ActorSystem, Props, Status}
import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout

import scala.collection.immutable
import scala.concurrent.duration._

final case class PartitionedGroup(routeePaths: immutable.Iterable[String]) extends Group {

  override def paths(system: ActorSystem) = routeePaths

  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId

  override def createRouter(system: ActorSystem): Router = {
    val log = Logging.getLogger(system, this)
    log.info("Creating PartitionedGroup router")
    val logic = new RoutingLogic {
      val roundRobin = RoundRobinRoutingLogic()

      def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
        //TODO some other logic
        val selected = roundRobin.select(message, routees)
        if (log.isDebugEnabled) {
          log.debug("partition selected: " + selected)
        }
        selected
      }
    }
    new Router(logic)
  }

}

class Gateway extends Actor {

  import context.dispatcher

  val log = Logging.getLogger(context.system, this)

  val uuid = UUID.randomUUID()

  val inputMediator = context.actorOf(Props(new UserInputMediator))

  val shardPaths = List(
    "akka.tcp://MySymmetricCluster@127.0.0.1:2551/user/shard-partition-1",
    "akka.tcp://MySymmetricCluster@127.0.0.1:2551/user/shard-partition-3"
  )

  val partitioner = context.actorOf(PartitionedGroup(shardPaths).props(), name = "partitioner")


  override def preStart(): Unit = {
    log.info("Gateway Starting")
  }

  def receive = {
    case HttpRequest(_, Uri.Path("/error"), _, _, _) =>
      sender ! Status.Failure(new IllegalStateException())

    case HttpRequest(GET, Uri.Path("/hi"), _, _, _) =>
      val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
        "<html><body><h2>Please wait..</h2>" +
          "<script>setTimeout(\"location.href = '/hello';\",100);</script></body></html>")
      sender ! HttpResponse(entity = entity)
//      val found = "/hello"
//      val locationHeader = headers.Location(found)
//      HttpResponse(headers = List(locationHeader),   ..


    case HttpRequest(GET, Uri.Path("/hello"), _, _, _) =>
      val originalSender = sender()
      implicit val timeout = Timeout(60 seconds)
      partitioner ? "hello" onSuccess {
        case justText: String => {
          originalSender ! HttpResponse(
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + justText + "</h1>"))
        }
      }

    case HttpRequest(_, _, _, _, _) =>
      sender ! HttpResponse(status = StatusCodes.NotFound,
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Haven't got that</h1>"))

//    case id: akka.actor.ActorIdentity => if (id.correlationId == uuid) {
//      log.debug("ActorIdentity " + id.getRef)
//    }

    case _ => sender ! Status.Failure(new IllegalArgumentException)

  }

}
