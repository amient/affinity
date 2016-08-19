package io.amient.akkahttp.actors

import akka.actor.{Actor, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

object Partition {
  case class Entry(key: String, value: String)
}

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
    case "hello" =>
      implicit val timeout = Timeout(60 seconds)
      val originalSender = sender()
      for(userInput <- userInputMediator ? "hello") {
        originalSender ! userInput
      }

    case Partition.Entry(key, value) =>
      println(s"Shard received: ($key, $value)")
      cache += (key -> value)

    case key: String =>
      sender ! cache.get(key)
  }
}

