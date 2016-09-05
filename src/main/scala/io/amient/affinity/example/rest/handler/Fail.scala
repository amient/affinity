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

package io.amient.affinity.example.rest.handler

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.example.rest.HttpGateway

import scala.concurrent.duration._

trait Fail extends HttpGateway {

  import context.dispatcher

  abstract override def receive: Receive = super.receive orElse {

    case HTTP(GET, Path("/fail"), _, response) =>
      implicit val timeout = Timeout(1 second)
      cluster ! new IllegalStateException
      response.success(HttpResponse(status = StatusCodes.Accepted))

    case HTTP(GET, Path("/error"), _, response) =>
      implicit val timeout = Timeout(1 second)
      val task = cluster ? "message-that-can't-be-handled"
      fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
        case any => jsonValue(OK, any)
      }

    case HTTP(GET, Uri.Path("/kill"), query, response) =>
      implicit val timeout = Timeout(1 second)
      query.get("p") match {
        case Some(p) if (p.toInt >= 0) =>
          val task = cluster ? (p.toInt, "kill-node")
          fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
            case any => HttpResponse(status = StatusCodes.Accepted)
          }
        case _ => response.success(errorValue(BadRequest,
          ContentTypes.`application/json`, "query string param p is required and must be >=0"))
      }

  }

}
