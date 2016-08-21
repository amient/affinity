package io.amient.akkahttp.actor

import java.util.{Properties, UUID}

import akka.actor.{Actor, ActorPath, Props, Status}
import akka.event.Logging
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.akkahttp.Coordinator
import io.amient.akkahttp.actor.Partition.{CollectUserInput, KillNode, TestError}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal

object Gateway {
  final val CONFIG_AKKA_HOST = "gateway.akka.host"
  final val CONFIG_AKKA_PORT = "gateway.akka.port"
  final val CONFIG_NUM_PARTITIONS = "num.partitions"
  final val CONFIG_PARTITION_LIST = "partition.list"

  final case class HttpExchange(
             method:   HttpMethod,
             uri:      Uri,
             headers:  immutable.Seq[HttpHeader],
             entity:   RequestEntity,
             promise: Promise[HttpResponse]) {
  }
}

class Gateway(appConfig: Properties) extends Actor {

  //TODO gateway must be able to route before the local partitions are online
  //TODO after the local partitions are online they must tell it to the coordinator
  //which then updates all other gateway nodes that are online

  import Gateway._

  val host = appConfig.getProperty(CONFIG_AKKA_HOST, "localhost")
  val akkaPort = appConfig.getProperty(CONFIG_AKKA_PORT, "2552").toInt
  val numPartitions = appConfig.getProperty(CONFIG_NUM_PARTITIONS).toInt
  val partitionList = appConfig.getProperty(CONFIG_PARTITION_LIST).split("\\,").map(_.toInt).toList

  val coordinator = Coordinator.fromProperties(appConfig)

  val log = Logging.getLogger(context.system, this)

  val uuid = UUID.randomUUID()

  val partitioner = context.actorOf(PartitionedGroup(numPartitions).props(), name = "partitioner")

  //construct partition actors
  val partitions = for (p <- partitionList) yield {
    context.actorOf(Props(new Partition(p)), name = "partition-" + p)
  }

  //create coorinator and register the partitions
  coordinator.watchRoutees(context.system, self)

  import context.dispatcher

  //register partitions that have booted successfully
  val handles = for (p <- partitions) yield {
    val partitionActorPath = ActorPath.fromString(
      s"akka.tcp://${context.system.name}@${host}:${akkaPort}${p.path.toStringWithoutAddress}")
    implicit val timeout = Timeout(24 hours)
    (for (partitionActorRef <- context.actorSelection(partitionActorPath).resolveOne()) yield {
      coordinator.register(partitionActorPath)
    }) recover {
      case any => throw new RuntimeException(any)
    }
  }


  override def preStart(): Unit = {
    log.info("Gateway Starting")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    println("preRestart Gateway")
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    println("postRestart Gateway")
  }


  override def postStop(): Unit = {
    Await.result(Future.sequence(handles), 1 minute).foreach(coordinator.unregister)
    log.info("closing coordinator")
    coordinator.close()
  }


  def receive = {

    //non-delegate response
    case HttpExchange(GET, Uri.Path("/") | Uri.Path(""), _, _, promise) =>
      promise.success(HttpResponse(
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Hello World!</h1>")))


    //delegate query path
    case HttpExchange(GET, uri, _, _, promise) => uri match {

      case Uri.Path("/error") =>
        implicit val timeout = Timeout(1 second)
        fulfillAndHandleErrors(promise, partitioner ? TestError) {
          case x => sys.error("Expecting failure, got" + x)
        }

      case Uri.Path("/kill") =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", x)) if (x.toInt >= 0) =>
            fulfillAndHandleErrors(promise, partitioner ? KillNode(x.toInt)) {
              case any => HttpResponse(status = StatusCodes.Accepted)
            }
          case _ => badRequest(promise, "query string param p must be >=0")
        }

      case Uri.Path("/hello") =>
        implicit val timeout = Timeout(60 seconds)
        fulfillAndHandleErrors(promise, partitioner ? CollectUserInput(uri.queryString().getOrElse(null))) {
          case justText: String => HttpResponse(
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + justText + "</h1>"))

          case s => HttpResponse(status = StatusCodes.NotAcceptable,
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Can't give you that: " + s.getClass + "</h1>"))
        }

      case Uri.Path("/hi") =>
        promise.success(HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
          "<html><body><h2>Please wait..</h2>" +
            "<script>setTimeout(\"location.href = '/hello?" + uri.query() + "';\",100);" +
            "</script></body></html>")))

      case _ =>
        promise.success(HttpResponse(status = StatusCodes.NotFound,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Haven't got that</h1>")))
    }


    //TODO command path (POST)


    case HttpExchange(_, _, _, _, promise) =>
      promise.success(HttpResponse(status = StatusCodes.MethodNotAllowed,
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Can't do that here </h1>")))

    //Management queries
    case m: AddRoutee =>
      log.info("Adding partition: " + m.routee)
      partitioner ! m

    case m: RemoveRoutee =>
      log.info("Removing partition: " + m.routee)
      partitioner ! m

    case m: GetRoutees =>
      val origin = sender()
      implicit val timeout = Timeout(60 seconds)
      partitioner ? m onSuccess { case routees => origin ! routees }

    case _ => sender ! Status.Failure(new IllegalArgumentException)

  }

  def badRequest(promise: Promise[HttpResponse], message: String) = {
    promise.success(HttpResponse(status = StatusCodes.BadRequest,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>$message</h1>")))
  }
  def fulfillAndHandleErrors(promise: Promise[HttpResponse], future: Future[Any])(f: Any => HttpResponse) = {
    promise.completeWith(handleErrors(future, f))
  }

  def handleErrors(future: Future[Any], f: Any => HttpResponse): Future[HttpResponse] = {
    future map(f) recover {
      case e: IllegalArgumentException =>
        log.error("Gateway contains bug! ", e)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> Eeek! We have a bug..</h1>"))

      case NonFatal(e) =>
        log.error("Cluster encountered failure ", e)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1> Well, something went wrong but we should be back..</h1>"))

      case e =>
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1> Something is seriously wrong with our servers..</h1>"))

    }
  }

}
