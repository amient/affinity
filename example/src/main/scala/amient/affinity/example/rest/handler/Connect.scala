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
import akka.http.scaladsl.model.{ContentTypes, Uri}
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.example.data.{Component, Vertex}
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.service.UserInputMediator

import scala.concurrent.duration._

trait Connect extends HttpGateway {

  import context.dispatcher

  implicit val timeout = Timeout(10 seconds)

  abstract override def handle: Receive = super.handle orElse {

    case HTTP(GET, PATH("com", INT(id), INT(id2)), query, response) =>
      val source = Vertex(id)
      val target = Vertex(id2)
      val task = cluster ? Component(source, Set(target))
      fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
        case true => redirect(SeeOther, Uri(s"/com/$id"))
        case false => redirect(MovedPermanently, Uri(s"/com/$id"))
      }

    case HTTP(GET, PATH("com", INT(id)), query, response) =>
      val task = cluster ? Vertex(id)
      fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
        case null => errorValue(NotFound, "Vertex not found")
        case component => jsonValue(OK, component)
      }

    case HTTP(GET, PATH("delegate"), query, response) =>
      val userInputMediator = service(classOf[UserInputMediator])
      userInputMediator ? "hello" onSuccess {
        case userInput: String => response.success(htmlValue(OK, userInput))
      }

  }

}
