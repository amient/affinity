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

package io.amient.affinity.example

import akka.http.javadsl.model.headers._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.core.cluster.CoordinatorZk
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.TimeCryptoProofSHA256
import io.amient.affinity.example.http.handler.{Admin, PublicApi}
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.rest.handler.Ping
import io.amient.affinity.testutil.SystemTestBaseWithKafka
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ExampleSystemTest extends FlatSpec with SystemTestBaseWithKafka with Matchers {

  import KafkaStorage._
  import State._

  val config = configure("example")
    .withValue(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
    .withValue(s"${CONFIG_STATE_STORE("settings")}.$CONFIG_KAFKA_BOOTSTRAP_SERVERS", ConfigValueFactory.fromAnyRef(kafkaBootstrap))
    .withValue(s"${CONFIG_STATE_STORE("graph")}.$CONFIG_KAFKA_BOOTSTRAP_SERVERS", ConfigValueFactory.fromAnyRef(kafkaBootstrap))

  val gateway = new TestGatewayNode(config, new HttpGateway
    with Ping
    with Admin
    with PublicApi
  )

  import gateway._

  val mapper = new ObjectMapper()


  "ExampleApp Gateway" should "be able to play ping pong" in {
    http_get(uri("/ping")).entity should be(jsonStringEntity("pong"))
  }

  "Admin requests" should "be authenticated with Basic Auth" in {
    val response1 = http_get(uri("/settings"))
    response1.status should be (Unauthorized)
    val authHeader = response1.header[WWWAuthenticate].get
    val challenge = authHeader.getChallenges.iterator().next
    challenge.realm() should be ("Create admin password")
    challenge.scheme() should be ("BASIC")
    http_get(uri("/settings"), List(Authorization.basic("admin", "1234"))).status should be (OK)
    http_get(uri("/settings"), List(Authorization.basic("admin", "wrong-password"))).status should be (Unauthorized)
  }

  "Public API requests" should "be allowed only with valid salted and time-based signature" in {
    val publicKey = "pkey1"
    val createApiKey = http_post(uri(s"/settings/add?key=$publicKey"), "", List(Authorization.basic("admin", "1234")))
    createApiKey.status should be (OK)
    implicit val materializer = ActorMaterializer.create(gateway.system)
    val json = Await.result(createApiKey.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
    val salt = mapper.readValue(json, classOf[String])
    val crypto = new TimeCryptoProofSHA256(salt)

    val requestUrl = uri("/profile/mypii")
    //unsigned request should be rejected
    val response1 = http_get(requestUrl)
    response1.status should be (Unauthorized)

    //signed request should have a valid response
    val requestSignature = crypto.sign(requestUrl.path.toString)
    val glue = if (requestUrl.rawQueryString.isDefined) "&" else "?"
    val signedRequestUrl = Uri(requestUrl + glue + "signature=" + publicKey + ":" + requestSignature)
    val response2 = http_get(signedRequestUrl)
    response2.status should be (OK)

    //the response should also be signed by the server and the response signature must be valid
    val json2 = Await.result(response2.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
    val jsonNode = mapper.readValue(json2, classOf[JsonNode])
    jsonNode.get("pii").textValue() should be("mypii")
    val responseSignature = jsonNode.get("signature").textValue()
    crypto.verify(responseSignature, requestSignature + "!")

  }

  //FIXME test consistency of connect/disconnect features under failure/rollback scenarios
  // have noticed inconsistent state when encountering failures while experimenting with websockets


  //TODO time-based DSA implementation and test, i.e. instead of shared salt, there will be a proper private key

}
