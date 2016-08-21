package io.amient.akkahttp.actor

import akka.actor.{Actor, Props, Status}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Partition {
  final case class SimulateError()
  case class CollectUserInput(greeting: String)

  sealed abstract class Keyed[K](key: K) extends Serializable {
    override def hashCode() = key.hashCode()
  }

  final case class ShowIndex(key: String) extends Keyed[String](key)
  final case class KillNode(partition: Int) extends Keyed[Int](partition)

  case class StringEntry(key: String, value: String) extends Keyed[String](key)
  case class GetStringEntry(key: String) extends Keyed[String](key)
}

import io.amient.akkahttp.actor.Partition._


class Partition(partition: Int) extends Actor {

  import context.dispatcher

  val log = Logging.getLogger(context.system, this)

  val id = self.path.toString

  var cache = Map.empty[String, String]

  val userInputMediator = context.actorOf(Props(new UserInputMediator))

  override def preStart(): Unit = {
    log.info("Partition Actor Starting: " + self.path.name)
  }

  override def postStop(): Unit = {
    log.info("stopping Partition: " + self.path.name)
    super.postStop()
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.info("preRestart Partition: " + self.path.name)
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    log.info("postRestart Partition: " + self.path.name)
  }


  def receive = {

    case SimulateError =>
      log.warning("TestError instruction in partition " + context.system.name)
      throw new IllegalStateException()

    case KillNode(_) =>
      log.warning("killing the entire node " + context.system.name)
      try {
        context.system.terminate()
      } finally {
        System.exit(666)
      }


    case ShowIndex(msg) =>
      sender ! s"${self.path.name}:$msg"

    case CollectUserInput(greeting) =>
      implicit val timeout = Timeout(60 seconds)
      val origin = sender()
      val prompt = s"${self.path.name}: $greeting >"
      userInputMediator ? prompt andThen {
        case Success(userInput: String) =>
          if (userInput == "error") origin ! Status.Failure(throw new RuntimeException(userInput))
          else origin ! userInput
        case Failure(e) => origin ! Status.Failure(e)
      }

    case StringEntry(key, value) =>
      val msg = s"${self.path.name}: PUT ($key, $value)"
      if (log.isDebugEnabled) {
        log.debug(msg)
      }
      cache += (key -> value)
      sender ! true

    case GetStringEntry(key) =>
      val value = cache.get(key)
      log.debug(s"GET: ($key, $value)")
      sender ! value

    case _ => sender ! Status.Failure(new IllegalArgumentException)
  }
}

