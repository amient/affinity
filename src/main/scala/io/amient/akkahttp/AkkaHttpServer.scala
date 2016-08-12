package io.amient.akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.io.StdIn
import scala.util.control.NonFatal

/**
  * Created by mharis on 12/08/2016.
  */
object AkkaHttpServer extends App {

  implicit val system = ActorSystem.create()

  implicit val materializer = ActorMaterializer.create(system)

  implicit val executionContext = system.dispatcher

  private def messagePromiseWithManualInput(): Future[String] = {
    val promise = Promise[String]
    Future {
      print("incoming user request, say hello: ")
      val line = StdIn.readLine()
      if (line.contains("error")) {
        promise.failure(new RuntimeException(line))
      } else {
        promise.success(line)
      }
    }
    promise.future
  }

  val internalErrorHandler = ExceptionHandler {
    case NonFatal(e) => complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> Something went wrong ..</h1>"))
    case _ => complete(HttpResponse(
      status = StatusCodes.InternalServerError,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> It's really bad ..</h1>")))
  }

  val route: Route = handleExceptions(internalErrorHandler) {
    path("hello") {
      get {
        onSuccess(messagePromiseWithManualInput()) {
          case line => complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + line + "</h1>"))
        }
      }
    } ~ path("hi") {
      get {
        val found = "/hello"
        val locationHeader = headers.Location(found)
        val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
          "<html><body><h2>Please wait..</h2>" +
            "<script>setTimeout(\"location.href = '" + found + "';\",100);</script></body></html>")
        complete(HttpResponse(headers = List(locationHeader), entity = entity))
      } ~ post {
        complete(StatusCodes.MethodNotAllowed)
      }
    }
  }

  val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(route, "localhost", 8080)

  println(s"Akka Http Server online at http://localhost:8080/\nPress ^C to stop...")

  val binding: ServerBinding = Await.result(bindingFuture, 10 seconds)
  sys.addShutdownHook {
    println("unbinding server port ...")
    Await.result(binding.unbind(), 10 seconds)
    println("server unbound, terminating actor system")
    system.terminate()
  }

}
