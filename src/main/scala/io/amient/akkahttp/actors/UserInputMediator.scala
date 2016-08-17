package io.amient.akkahttp.actors

import akka.actor.{Actor, Status}

import scala.io.StdIn


class UserInputMediator extends Actor {

//  import context.dispatcher
//
//  private def messagePromiseWithManualInput(): Future[String] = {
//    val promise = Promise[String]
//    Future {
//      print("incoming user request, say hello: ")
//      val line = StdIn.readLine()
//      if (line.contains("error")) {
//        promise.failure(new RuntimeException(line))
//      } else {
//        promise.success(line)
//      }
//    }
//    promise.future
//  }

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
