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
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, UpgradeToWebSocket}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, INT, PATH, QUERY}
import io.amient.affinity.example.rest.HttpGateway

import scala.concurrent.duration._

trait WebSock extends HttpGateway {

  val html = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/wsclient.html")).mkString

  abstract override def handle: Receive = super.handle orElse {

    case HTTP(GET, PATH("vertex"), QUERY(("id", INT(vertex))), response) =>
      response.success(Encoder.html(OK, html))

    case http@HTTP(GET, PATH("listen"), QUERY(("id", INT(vertex))), response) => response.success {
      http.request.header[UpgradeToWebSocket] match {
        case Some(upgrade) => upgrade.handleMessagesWithSinkSource(input, output(vertex))
        case None => Encoder.html(BadRequest, "This is a web socket handler!")
      }
    }
  }

  implicit val materializer = ActorMaterializer.create(context.system)

  val input = Sink.foreach[Message] {
    case tm: TextMessage => println(tm.getStrictText)
    case bm: BinaryMessage => println("Unsupported binary message " + bm)
  }
  def output(v: Int) = //Source.fromIterator[Message](() => Iterator.continually(TextMessage(scala.io.StdIn. readLine())))
    Source(0 to 1000)
      .map(i => TextMessage(v + ": " + i.toString))
      .throttle(1, 3 seconds, 1, ThrottleMode.shaping)
}
