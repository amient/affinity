/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.example.symmetric

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.event.Logging
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.Handler
import io.amient.affinity.example.symmetric.actor.UserInputMediator

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

//TODO maybe instead of mixing Actor with Handler try annotation mechanism

object RequestHandler {

  final case class ShowIndex(ts: Long = System.currentTimeMillis())

  final case class KillNode(partition: Int) {
    override def hashCode = partition
  }

  final case class CollectUserInput(greeting: String)

  final case class StringEntry(key: String, value: Option[String]) {
    override def hashCode = key.hashCode
  }

  final case class GetStringEntry(key: String) {
    override def hashCode = key.hashCode
  }

}

class LocalHandler extends Actor {

  val log = Logging.getLogger(context.system, this)

  import context._

  import RequestHandler._

  val userInputMediator = actorOf(Props(new UserInputMediator))

  val cluster = context.actorSelection("/user/controller/gateway/cluster")

  //TODO this is a prototype for the embedded storage API which will be supplied via core.Partition
  var cache = scala.collection.mutable.Map[String, String]()

  def receive = {
    case ShowIndex(ts) =>
      val origin = sender
      implicit val timeout = Timeout(10 seconds)
      val randomKey = ts.toString
      cluster ! StringEntry(randomKey, Some("world"))
      cluster ? GetStringEntry(randomKey) onSuccess {
        case any => origin ! s"${parent.path.name}:Hello World!: " + any
      }


    case KillNode(_) =>
      log.warning("killing the entire node " + system.name)
      implicit val timeout = Timeout(10 seconds)
      actorSelection("/user").resolveOne().onSuccess {
        case controller => stop(controller)
      }

    case CollectUserInput(greeting) =>
      implicit val timeout = Timeout(60 seconds)
      val origin = sender()
      val prompt = s"${parent.path.name}: $greeting >"
      userInputMediator ? prompt andThen {
        case Success(userInput: String) =>
          if (userInput == "error") origin ! Status.Failure(throw new RuntimeException(userInput))
          else origin ! userInput
        case Failure(e) => origin ! Status.Failure(e)
      }

    case StringEntry(key, value) =>
      val msg = s"${parent.path.name}: PUT ($key, $value)"
      if (log.isDebugEnabled) {
        log.debug(msg)
      }

      if (value.isEmpty) cache -= key else value.foreach {
        case data => cache += (key -> data)
      }

      sender ! true

    case GetStringEntry(key) => sender ! StringEntry(key, cache.get(key))

    case unknown => sender ! Status.Failure(new IllegalArgumentException(unknown.getClass.getName))

  }
}

class RequestHandler extends Handler {

  import RequestHandler._


  def apply(request: HttpRequest, response: Promise[HttpResponse], cluster: ActorRef)(implicit ctx: ExecutionContext) {
    request match {
      //non-delegated fulfillment
      case HttpRequest(GET, Uri(_, _, Uri.Path("/hi"), rawQueryString, _), _, _, _) =>
        response.success(HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
          "<html><body><h2>Please wait..</h2>" +
            "<script>setTimeout(\"location.href = '/hello" + rawQueryString.map("?" + _).getOrElse("") + "';\",100);" +
            "</script></body></html>")))

      //delegated fulfillment
      case HttpRequest(GET, Uri.Path("/") | Uri.Path(""), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        fulfillAndHandleErrors(response, cluster ? ShowIndex()) {
          case any => HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>$any</h1>"))
        }

      case HttpRequest(GET, Uri.Path("/error"), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        cluster ! new IllegalStateException
        response.success(HttpResponse(status = StatusCodes.Accepted))

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/kill")) =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", x)) if (x.toInt >= 0) =>
            fulfillAndHandleErrors(response, cluster ? KillNode(x.toInt)) {
              case any => HttpResponse(status = StatusCodes.Accepted)
            }
          case _ => badRequest(response, "query string param p must be >=0")
        }

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/hello")) =>
        implicit val timeout = Timeout(60 seconds)
        fulfillAndHandleErrors(response, cluster ? CollectUserInput(uri.queryString().getOrElse(null))) {
          case justText: String => HttpResponse(
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + justText + "</h1>"))

          case s => HttpResponse(status = StatusCodes.NotAcceptable,
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Can't give you that: " + s.getClass + "</h1>"))
        }


      case _ =>
        response.success(HttpResponse(status = StatusCodes.NotFound,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Haven't got that</h1>")))

    }
  }

  def badRequest(promise: Promise[HttpResponse], message: String) = {
    promise.success(HttpResponse(status = StatusCodes.BadRequest,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>$message</h1>")))
  }

  def fulfillAndHandleErrors(promise: Promise[HttpResponse], future: Future[Any])
                            (f: Any => HttpResponse)(implicit ctx: ExecutionContext) = {
    promise.completeWith(handleErrors(future, f))
  }

  def handleErrors(future: Future[Any], f: Any => HttpResponse)(implicit ctx: ExecutionContext): Future[HttpResponse] = {
    future map (f) recover {
      case e: IllegalArgumentException =>
        e.printStackTrace() //log.error("Gateway contains bug! ", e)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> Eeek! We have a bug..</h1>"))

      case NonFatal(e) =>
        e.printStackTrace() //log.error("Cluster encountered failure ", e.getMessage)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1> Well, something went wrong but we should be back..</h1>"))

      case e =>
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1> Something is seriously wrong with our servers..</h1>"))

    }
  }

}
