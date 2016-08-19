package io.amient.akkahttp.actors

import akka.actor.{Actor, Status}

import scala.io.StdIn


class UserInputMediator extends Actor {

  override def receive: Receive = {
    case "hello" =>
      print("incoming user request, say hello: ")
      val line = StdIn.readLine()
      if (line.contains("error")) {
        sender ! Status.Failure(new RuntimeException(line))
      } else {
        sender ! line
      }
  }

}
