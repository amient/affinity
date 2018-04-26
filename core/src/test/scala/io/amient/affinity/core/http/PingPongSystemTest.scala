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

package io.amient.affinity.core.http

import akka.http.javadsl.model.ContentTypes
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp, Partition}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.util.{AffinityTestBase, Scatter}
import org.codehaus.jackson.map.ObjectMapper
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.language.{existentials, implicitConversions, postfixOps}

case class ClusterPing() extends Scatter[String] {
  override def gather(r1: String, r2: String) = r2
}

class PingPongSystemTest extends FlatSpec with AffinityTestBase with BeforeAndAfterAll with Matchers {

  def config = configure("pingpong")

  val node1 = new Node(config)
  node1.startGateway(new GatewayHttp {

    import context.dispatcher

    implicit val materializer = ActorMaterializer.create(context.system)

    implicit val scheduler = context.system.scheduler

    val ks = keyspace("region")

    object ERROR {
      def unapply(http: HttpExchange): Option[HttpExchange] = {
        throw new IllegalArgumentException(s"Simulated resumable exception ${http.request.method}")
      }
    }

    override def handle: Receive = {
      case HTTP(GET, PATH("ping"), _, response) => response.success(Encoder.json(OK, "pong", gzip = false))
      case http@HTTP(GET, PATH("timeout"), _, _) if http.timeout(200 millis) =>
      case HTTP(GET, PATH("clusterping"), _, response) => handleWith(response) {
        implicit val timeout = Timeout(1 second)
        ks ??? ClusterPing() map {
          case pong => Encoder.json(OK, pong, gzip = false)
        }
      }
      case HTTP_POST(ContentTypes.APPLICATION_JSON, entity, PATH("ping"), _, response) =>
        response.success(Encoder.json(OK, Decoder.json(entity), gzip = true))

      case HTTP(GET, PATH("restartable"), _, _) => throw new Exception("Simulated restartable exception")

      case ERROR(HTTP(GET, PATH("resumable"), _, response)) => response.success(HttpResponse(OK))
    }
  })

  val node2 = new Node(config)

  override protected def beforeAll(): Unit = try {
    node2.startContainer("region", List(0, 1), new Partition {
      override def handle: Receive = {
        case req@ClusterPing() => req(sender) ! "pong"
      }
    })
    node1.awaitClusterReady()
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node1.shutdown()
    node2.shutdown()
  } finally {
    super.afterAll()
  }


  "A Simple Gateway" should "play ping pong well" in {
    node1.http_get("/ping").entity should be(jsonStringEntity("pong"))
  }

  "A Simple Cluster" should "play ping pong too" in {
    node1.http_get("/clusterping").entity should be(jsonStringEntity("pong"))
  }

  "A Simple Handler" should "be able to change http timeout dynamically" in {
    val t = System.currentTimeMillis()
    val response = node1.http_get("/timeout")
    response.status should be(ServiceUnavailable)
    response.entity.toString.contains("The server was not able to produce a timely response") should be(true)
    (System.currentTimeMillis() - t) should be < 1000L
  }

  "An Entity Handler" should "be able to stream decode json entity" in {
    val json = new ObjectMapper().createObjectNode()
    json.put("hello", "hello")
    Encoder.json(json) should be("{\"hello\":\"hello\"}")
    node1.get_json(node1.http_post_json("/ping", json)) should be(json)
  }

  "Gateway" should "not restart after illegal argument exception from a handler" in {
    node1.http(HttpRequest(HttpMethods.GET, node1.uri("/resumable")))
    implicit val system = node1.system
    Http().shutdownAllConnectionPools()
    node1.http_get("/ping").status should be (OK)
  }
  
}
