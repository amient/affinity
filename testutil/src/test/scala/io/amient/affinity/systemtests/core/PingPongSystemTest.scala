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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.actor.{Gateway, Partition}
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.testutil.SystemTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class PingPongSystemTest extends FlatSpec with SystemTestBase with Matchers {

  val config = configure("systemtests")

  val gateway = new TestGatewayNode(config, new Gateway {
    import context.dispatcher
    override def handle: Receive = {
      case HTTP(GET, PATH("ping"), _, response) => response.success(Encoder.json(OK, "pong", gzip = false))
      case HTTP(GET, PATH("clusterping"), _, response) =>
        implicit val timeout = Timeout(1 second)
        delegateAndHandleErrors(response, cluster ? "ping") {
          case pong => Encoder.json(OK, pong, gzip = false)
        }
    }
  })

  val region = new TestRegionNode(config, new Partition {
    override def handle: Receive = {
      case "ping" => sender ! "pong"
    }
  })

  override def afterAll(): Unit = {
    try {
      gateway.shutdown()
      region.shutdown()
    } finally {
      super.afterAll()
    }
  }


  "A Simple Gateway" should "play ping pong well" in {
    gateway.http_sync(GET, s"/ping").entity should be(jsonStringEntity("pong"))
  }

  "A Simple Cluster" should "play ping pong too" in {
    gateway.http_sync(GET, s"/clusterping").entity should be(jsonStringEntity("pong"))
  }

}
