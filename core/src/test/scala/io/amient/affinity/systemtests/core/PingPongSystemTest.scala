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

package io.amient.affinity.systemtests.core

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.systemtests.SystemTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._


class PingPongSystemTest extends FlatSpec with SystemTestBase with Matchers {

  "A Simple Gateway" should "play ping pong well" in {
    val (node, httpPort) = createGatewayNode() {
        case HTTP(GET, PATH("ping"), _, response) => response.success(HttpResponse(OK, entity = "pong"))
    }

    import node.system
    implicit val materializer = ActorMaterializer()

    try {
      val response = Await.result(Http().singleRequest(HttpRequest(uri = s"http://localhost:$httpPort/ping")), 1 second)
      response.status should be(OK)
      response.entity should be(HttpEntity("pong"))

    } finally {
      node.shutdown()
    }

  }

}
