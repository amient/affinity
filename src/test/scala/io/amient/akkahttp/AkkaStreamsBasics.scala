package io.amient.akkahttp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.io.StdIn


object AkkaStreamsBasics extends App {

  implicit val system = ActorSystem.create()

  implicit val materializer = ActorMaterializer.create(system)

  implicit val executionContext = system.dispatcher

  val autoincrementSource = Source.fromIterator(() => {
    var n = 0
    Iterator.continually {
      n += 1
      n
    }
  })

  val flowDoublingElements = Flow[Int].map(_ * 2)

  autoincrementSource.via(flowDoublingElements).runWith(Sink.foreach[Int] { f =>
    println(f)
  })

  StdIn.readLine()

  system.terminate()
}