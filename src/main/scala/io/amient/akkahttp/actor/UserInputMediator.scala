package io.amient.akkahttp.actor

import akka.actor.{Actor, Status}

import scala.io.StdIn


class UserInputMediator extends Actor {

  override def receive: Receive = {
    case greeting: String =>
      require(greeting != null && !greeting.isEmpty, "User Mediator requires non-empty greeting")
      print(s"'$greeting', please reply: ")
      //IOException on the next line may result in this actor being restarted
      sender ! StdIn.readLine()

    case _ =>
      //not a fault of this actor, sender's bad
      sender ! Status.Failure(new IllegalArgumentException("UserInputMediator only understands String messages"))
  }

}
