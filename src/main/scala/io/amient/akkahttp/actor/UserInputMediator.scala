package io.amient.akkahttp.actor

import akka.actor.{Actor, Status}

import scala.io.StdIn


class UserInputMediator extends Actor {

  override def receive: Receive = {
    case greeting:String =>
      require(greeting != null && !greeting.isEmpty, "User Mediator requires non-empty greeting")
      print(s"'$greeting', please reply: ")
      try {
        val line = StdIn.readLine()
        if (line.contains("error")) {
          throw new RuntimeException(line)
        }
        sender ! line
      } catch {
        case e: Exception =>  sender ! Status.Failure(e)
      }

    case _ => sender ! Status.Failure(new IllegalArgumentException("UserInputMediator only understands String messages"))
  }

}
