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

package io.amient.affinity.systemtests.http

import akka.http.javadsl.model.ContentTypes
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.fasterxml.jackson.databind.ObjectMapper
import io.amient.affinity.core.actor.{GatewayHttp, Partition}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.{Decoder, Encoder}
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.testutil.SystemTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.language.{existentials, implicitConversions, postfixOps}

class PingPongSystemTest extends FlatSpec with SystemTestBase with Matchers {

  val config = configure("pingpong")


  val gateway = new TestGatewayNode(config, new GatewayHttp {

    import context.dispatcher

    implicit val materializer = ActorMaterializer.create(context.system)

    val regionService = service("region")


    override def handle: Receive = {
      case HTTP(GET, PATH("ping"), _, response) => response.success(Encoder.json(OK, "pong", gzip = false))
      case http@HTTP(GET, PATH("timeout"), _, response) if http.timeout(200 millis) =>
      case HTTP(GET, PATH("clusterping"), _, response) =>
        implicit val timeout = Timeout(1 second)
        delegateAndHandleErrors(response, regionService ? "ping") {
          case pong => Encoder.json(OK, pong, gzip = false)
        }
      case HTTP_POST(ContentTypes.APPLICATION_JSON, entity, PATH("ping"), _, response) =>
        response.success(Encoder.json(OK, Decoder.json(entity), gzip = true))
    }
  })

  val region = new Node(config)

  gateway.awaitClusterReady {
    region.startContainer("region", List(0,1), new Partition {
      override def handle: Receive = {
        case "ping" => sender ! "pong"
      }
    })
  }

  override def afterAll(): Unit = {
    try {
      gateway.shutdown()
      region.shutdown()
    } finally {
      super.afterAll()
    }
  }


  "A Simple Gateway" should "play ping pong well" in {
    gateway.http_get(gateway.uri("/ping")).entity should be(jsonStringEntity("pong"))
  }

  "A Simple Cluster" should "play ping pong too" in {
    gateway.http_get(gateway.uri("/clusterping")).entity should be(jsonStringEntity("pong"))
  }

  "A Simple Handler" should "be able to change http timeout dynamically" in {
    val t = System.currentTimeMillis()
    val response = gateway.http_get(gateway.uri("/timeout"))
    response.status should be(ServiceUnavailable)
    response.entity.toString.contains("The server was not able to produce a timely response") should be (true)
    (System.currentTimeMillis() - t) should be < 1000L
  }

  "An Entity Handler" should "be able to stream decode json entity" in {
    val json = new ObjectMapper().createObjectNode()
    json.put("hello", "hello")
    gateway.get_json(gateway.http_post_json(gateway.uri("/ping"), json)) should be(json)
  }
}
