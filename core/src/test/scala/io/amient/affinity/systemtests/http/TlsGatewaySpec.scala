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

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import akka.util.Timeout
import io.amient.affinity.core.actor.{Controller, GatewayHttp, GatewayApi}
import io.amient.affinity.core.http.RequestMatchers.HTTP
import io.amient.affinity.testutil.SystemTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

class TlsGatewaySpec extends FlatSpec with SystemTestBase with Matchers {

  val config = configure("tlstests")
  val system = ActorSystem.create("TlsGatewayTest", config)

  val gateway = new TestGatewayNode(config, new GatewayHttp {
    override def handle: Receive = {
      case http@HTTP(GET, _, _, response) => response.success(HttpResponse(OK, entity = "Hello World"))
    }
  })

  override def afterAll(): Unit = {
    try {
      gateway.shutdown()
    } finally {
      super.afterAll()
    }
  }

  "HTTPS Requests" should "be handled correctly as TLS" in {
    gateway.http_get(gateway.https_uri("/tls-hello")).entity should equal(HttpEntity("Hello World"))
  }

}
