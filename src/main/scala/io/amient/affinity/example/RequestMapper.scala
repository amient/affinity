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
import scala.concurrent.{ExecutionContext, Promise}

class RequestMapper extends HttpRequestMapper {

  def apply(request: HttpRequest, response: Promise[HttpResponse], cluster: ActorRef)(implicit ctx: ExecutionContext) {

    request match {

      //handlers without filter
      case HttpRequest(GET, Uri.Path("/ping"), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        val task = cluster ? (System.currentTimeMillis(), "ping")
        fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
          case any => jsonValue(OK, any)
        }

      //handlers with filter
      case _ => applyFiltered(request, response, cluster)

    }
  }

  def applyFiltered(request: HttpRequest, response: Promise[HttpResponse], cluster: ActorRef)(implicit ctx: ExecutionContext): Unit = {

    //filter
    val query = request.uri.query().toMap
    if (!query.contains("signature")) {
      response.success(errorValue(Forbidden, ContentTypes.`application/json`, "Forbidden"))
    }

    //handlers
    if (!response.isCompleted) request match {
      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/")) =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", p)) if (p.toInt >= 0) =>
            val task = cluster ? (p.toInt, "describe")
            fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
              case any => jsonValue(OK, any)
            }
          case _ => response.success(errorValue(BadRequest,
            ContentTypes.`application/json`, "query string param p must be >=0"))
        }

      case HttpRequest(GET, Uri.Path("/fail"), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        cluster ! new IllegalStateException
        response.success(HttpResponse(status = StatusCodes.Accepted))

      case HttpRequest(GET, Uri.Path("/error"), _, _, _) =>
        implicit val timeout = Timeout(1 second)
        val task = cluster ? "message-that-can't-be-handled"
        fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
          case any => jsonValue(OK, any)
        }

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/kill")) =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", p)) if (p.toInt >= 0) =>
            val task = cluster ? (p.toInt, "kill-node")
            fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
              case any => HttpResponse(status = StatusCodes.Accepted)
            }
          case _ => response.success(errorValue(BadRequest,
            ContentTypes.`application/json`, "query string param p must be >=0"))
        }

      case HttpRequest(GET, uri, _, _, _) if (uri.path == Uri.Path("/connect")) =>
        implicit val timeout = Timeout(60 seconds)
        val task = cluster ? (System.currentTimeMillis(), "collect-user-input")
        fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
          case userInput: String =>
            val Array(start, end) = userInput.split(",").map(_.toInt)
            val source = Vertex(start, "A")
            val target = Vertex(end, "B")
            cluster ! Edge(source, target)
            jsonValue(OK, s"Connecting vertex $start with $end")

          case s => errorValue(NotAcceptable,
            ContentTypes.`application/json`, "Can't give you that: " + s.getClass)
        }

      case _ => response.success(htmlValue(NotFound, "Haven't got that"))

    }

  }

}