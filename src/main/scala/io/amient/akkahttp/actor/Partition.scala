package io.amient.akkahttp.actor

import akka.actor.{Actor, Props, Status}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

object Partition {
  final case class TestError()
  case class CollectUserInput(greeting: String)

  sealed abstract class Keyed[K](key: K) extends Serializable {
    override def hashCode() = key.hashCode()
  }

  final case class KillNode(partition: Int) extends Keyed[Int](partition)

  case class StringEntry(key: String, value: String) extends Keyed[String](key)
  case class GetStringEntry(key: String) extends Keyed[String](key)
}

import Partition._


class Partition(partition: Int) extends Actor {

  import context.dispatcher

  val log = Logging.getLogger(context.system, this)

  val id = self.path.toString

  var cache = Map.empty[String, String]

  val userInputMediator = context.actorOf(Props(new UserInputMediator))

  override def preStart(): Unit = {
    log.info("Partition Actor Starting: " + self.path.name)
  }

  def receive = {

    case TestError => sender ! Status.Failure(new IllegalStateException())

    case KillNode(_) =>
      sender ! "OK"
      Thread.sleep(500)
      println("killing system "+  context.system.name)
      context.system.terminate()

    case CollectUserInput(greeting) =>
      implicit val timeout = Timeout(60 seconds)
      val origin = sender()
      val prompt = s"${self.path.name}: $greeting >"
      for (userInput <- userInputMediator ? prompt) origin ! userInput

    case StringEntry(key, value) =>
      val msg = s"${self.path.name}: PUT ($key, $value)"
      if (log.isDebugEnabled) {
        log.debug(msg)
      }
      println(msg)
      cache += (key -> value)
      sender ! true

    case GetStringEntry(key) =>
      val value = cache.get(key)
      log.debug(s"GET: ($key, $value)")
      sender ! value

    case _ => sender ! Status.Failure(new IllegalArgumentException)
  }
}

