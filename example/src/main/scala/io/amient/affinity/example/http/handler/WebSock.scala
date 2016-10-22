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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.ws.UpgradeToWebSocket
import io.amient.affinity.core.actor.WebSocketSupport
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.example.Edge
import io.amient.affinity.example.rest.HttpGateway

trait WebSock extends HttpGateway with WebSocketSupport {

  val html = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/wsclient.html")).mkString

  import context.dispatcher

  abstract override def handle: Receive = super.handle orElse {

    case http@HTTP(GET, PATH("vertex"), QUERY(("id", INT(vertex))), response) =>

      http.request.header[UpgradeToWebSocket] match {
        case None => response.success(Encoder.html(OK, html))
        case Some(upgrade) => fulfillAndHandleErrors(http.promise) {
          avroWebSocket(upgrade, "graph", vertex) {
            case x: Edge => println("My custom handler of Edge: " + x)
          }
        }
      }

  }

}

