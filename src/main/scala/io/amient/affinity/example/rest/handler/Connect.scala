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

import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Path
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.example.data.{Edge, Vertex}
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.service.UserInputMediator

import scala.concurrent.duration._

trait Connect extends HttpGateway {

  import context.dispatcher

  abstract override def receive: Receive = super.receive orElse {

    case HTTP(GET, Path("/connect"), query, response) =>
      implicit val timeout = Timeout(60 seconds)

      val userInputMediator = service(classOf[UserInputMediator])

      val task = userInputMediator ? "hello"
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
  }

}
