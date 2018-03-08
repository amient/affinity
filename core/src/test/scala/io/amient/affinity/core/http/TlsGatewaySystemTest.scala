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

import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import io.amient.affinity.AffinityActorSystem
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.RequestMatchers.HTTP
import io.amient.affinity.core.util.AffinityTestBase
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.language.postfixOps

class TlsGatewaySystemTest extends FlatSpec with AffinityTestBase with BeforeAndAfterAll with Matchers {

  def config = configure("tlstests")

  val system = AffinityActorSystem.create("TlsGatewayTest", config)

  val node = new Node(config)

  override def beforeAll: Unit = try {
    node.startGateway(new GatewayHttp {
      override def handle: Receive = {
        case HTTP(GET, _, _, response) => response.success(HttpResponse(OK, entity = "Hello World"))
      }
    })
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node.shutdown()
  } finally {
    super.afterAll()
  }

  "HTTPS Requests" should "be handled correctly as TLS" in {
    node.https_get("/tls-hello").entity should equal(HttpEntity("Hello World"))
  }

}
