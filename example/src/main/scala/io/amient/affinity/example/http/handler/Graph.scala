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

package io.amient.affinity.example.http.handler

import java.util.concurrent.ConcurrentHashMap

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.example._
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.service.UserInputMediator

import scala.concurrent.duration._

trait Graph extends HttpGateway {

  import context.dispatcher

  val cache = new ConcurrentHashMap[Int, HttpResponse]()

  abstract override def handle: Receive = super.handle orElse {

    /**
      * GET /vertex/<vertex>
      */
    case HTTP(GET, PATH("vertex", INT(vid)), query, response) =>
      implicit val timeout = Timeout(1 seconds)
      delegateAndHandleErrors(response, ack(cluster, GetVertexProps(vid))) {
        Encoder.json(OK, _)
      }

    /**
      * GET /component/<component>
      */
    case HTTP(GET, PATH("component", INT(cid)), query, response) =>
      implicit val timeout = Timeout(1 seconds)
      delegateAndHandleErrors(response, ack(cluster, GetComponent(cid))) {
        Encoder.json(OK, _)
      }

    /**
      * PUT /delegate
      */
    case HTTP(PUT, PATH("delegate"), query, response) =>
      val userInputMediator = service(classOf[UserInputMediator])
      implicit val timeout = Timeout(1 minute)
      userInputMediator ? "hello" onSuccess {
        case userInput: String => response.success(Encoder.json(OK, Map("userInput" -> userInput)))
      }

  }
}
