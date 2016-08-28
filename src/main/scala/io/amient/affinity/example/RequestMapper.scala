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

package io.amient.affinity.example

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.HttpRequestMapper
import io.amient.affinity.example.data.{Edge, Vertex}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

class RequestMapper extends HttpRequestMapper {

  def apply(request: HttpRequest, response: Promise[HttpResponse], cluster: ActorRef)(implicit ctx: ExecutionContext) {
    request match {

      case HttpRequest(GET, Uri.Path("/ping"), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        val task = cluster ? (System.currentTimeMillis(), "ping")
        fulfillAndHandleErrors(response, task) {
          case any => htmlMessage(OK, s"$any")
        }

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/")) =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", p)) if (p.toInt >= 0) =>
            val task = cluster ? (p.toInt, "describe")
            fulfillAndHandleErrors(response, task) {
              case any => htmlMessage(OK, s"$any")
            }
          case _ => response.success(htmlMessage(BadRequest, "query string param p must be >=0"))
        }

      case HttpRequest(GET, Uri.Path("/error"), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        cluster ! new IllegalStateException
        response.success(HttpResponse(status = StatusCodes.Accepted))

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/kill")) =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", p)) if (p.toInt >= 0) =>
            val task = cluster ? (p.toInt, "kill-node")
            fulfillAndHandleErrors(response, task) {
              case any => HttpResponse(status = StatusCodes.Accepted)
            }
          case _ => response.success(htmlMessage(BadRequest, "query string param p must be >=0"))
        }

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/connect")) =>
        implicit val timeout = Timeout(60 seconds)
        val task = cluster ? (System.currentTimeMillis(), "collect-user-input")
        fulfillAndHandleErrors(response, task) {
          case userInput: String =>
            val Array(start, end) = userInput.split(",").map(_.toInt)
            val source = Vertex(start, "A")
            val target = Vertex(end, "B")
            cluster ! Edge(source, target)
            htmlMessage(OK, s"Connecting vertex $start with $end")

          case s => htmlMessage(NotAcceptable, "Can't give you that: " + s.getClass )
        }

      case _ => response.success(htmlMessage(NotFound, "Haven't got that"))

    }
  }

  def htmlMessage(status: StatusCode, message: String): HttpResponse = {
    val formattedMessage = message.replace("\n", "<br/>")
    HttpResponse(status,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h3>$formattedMessage</h3s>"))
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

