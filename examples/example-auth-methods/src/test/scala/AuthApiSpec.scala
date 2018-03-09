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

import java.util.concurrent.atomic.AtomicReference

import akka.http.javadsl.model.headers._
import akka.http.scaladsl.model.StatusCodes._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.{AffinityTestBase, TimeCryptoProofSHA256}
import io.amient.affinity.ws.WebSocketClient
import io.amient.affinity.ws.WebSocketClient.AvroMessageHandler
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.codehaus.jackson.JsonNode
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class AuthApiSpec extends FlatSpec with AffinityTestBase with Matchers with BeforeAndAfterAll {

  val config = ConfigFactory.empty()
//    .withValue(Conf.Affi.Keyspace("graph").NumPartitions.path, "1")
//    .withValue(Conf.Affi.Node.Gateway.Http.Host.path, ConfigValueFactory.fromAnyRef("127.0.0.1"))
//    .withValue(Conf.Affi.Node.Gateway.Http.Port.path, ConfigValueFactory.fromAnyRef(0))


  val node1 = new Node(configure(config))

  override def beforeAll(): Unit = try {
    //node1.startGateway(new ExampleGateway)
//    node1.awaitClusterReady
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
//    node1.shutdown()
  } finally {
    super.afterAll()
  }

  "Admin requests" should "be authenticated with Basic Auth" ignore {
    val response1 = node1.http_get("/settings")
    response1.status should be(Unauthorized)
    val authHeader = response1.header[WWWAuthenticate].get
    val challenge = authHeader.getChallenges.iterator().next
    challenge.realm() should be("Create admin password")
    challenge.scheme() should be("BASIC")
    node1.http_get("/settings", List(Authorization.basic("admin", "1234"))).status should be(OK)
    node1.http_get("/settings", List(Authorization.basic("admin", "wrong-password"))).status should be(Unauthorized)
  }

  "Public API requests" should "be allowed only with valid salted and time-based signature" ignore {
    val publicKey = "pkey1"
    val createApiKey = node1.http_post(s"/settings/add?key=$publicKey", Array(), List(Authorization.basic("admin", "1234")))
    createApiKey.status should be(OK)
    implicit val materializer = ActorMaterializer.create(node1.system)
    val salt = node1.get_json(createApiKey).getTextValue
    val crypto = new TimeCryptoProofSHA256(salt)

    val requestPath = "/profile/mypii"
    //unsigned request should be rejected
    val response1 = node1.http_get(requestPath)
    response1.status should be(Unauthorized)

    //signed request should have a valid response
    val requestSignature = crypto.sign(requestPath)
    val signedRequestUrl = requestPath + "?signature=" + publicKey + ":" + requestSignature
    val response2 = node1.http_get(signedRequestUrl)
    response2.status should be(OK)

    //the response should also be signed by the server and the response signature must be valid
    val json2 = Await.result(response2.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
    val jsonNode = node1.mapper.readValue(json2, classOf[JsonNode])
    jsonNode.get("pii").getTextValue should be("mypii")
    val responseSignature = jsonNode.get("signature").getTextValue
    crypto.verify(responseSignature, requestSignature + "!")

  }

}
