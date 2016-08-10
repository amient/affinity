package io.amient.akka

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global

object Input {
  val e = new ThreadPoolExecutor(4, 4, 10L, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](100) {
    override def offer(e: Runnable): Boolean = try {
      put(e); true
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt(); false
    }})

  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(e)

  def close() = e.shutdownNow()

  val stream: Stream[Future[Int]] = ((1 to 100000).map(i => Future { i })).toStream
}

object Main extends App {

  implicit val system = ActorSystem.create()

  implicit val materializer = ActorMaterializer.create(system)

  //  val config = ConfigFactory.load()
  //  val logger = Logging(system, getClass)


  val src1 = Source[Future[Int]](Input.stream)

  val flowDoublingElements: Flow[Future[Int], Future[Int], NotUsed] = Flow[Future[Int]].map(_.map(_ * 2))

  src1.via(flowDoublingElements).runWith(Sink.foreach[Future[Int]] { f =>
    for (x <- f) println(x)
  }).onComplete { t: Try[Done] =>
    println("!!!!!!!!!!!!!!!")
//    Input.close()
    system.terminate()
  }

}