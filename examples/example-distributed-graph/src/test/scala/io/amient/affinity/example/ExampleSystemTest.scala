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

import java.net.URI
import java.util.concurrent.atomic.AtomicReference

import akka.http.javadsl.model.headers._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.TimeCryptoProofSHA256
import io.amient.affinity.example.http.handler.{Admin, Graph, PublicApi}
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.rest.handler.Ping
import io.amient.affinity.model.graph.GraphPartition
import io.amient.affinity.testutil.SystemTestBaseWithKafka
import io.amient.affinity.ws.AvroWebSocketClient
import io.amient.affinity.ws.AvroWebSocketClient.AvroMessageHandler
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ExampleSystemTest extends FlatSpec with SystemTestBaseWithKafka with Matchers {

  val config = configure("example").withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("ERROR"))

  val gateway = new TestGatewayNode(config, new HttpGateway
    with Ping
    with Admin
    with PublicApi
    with Graph)

  val region = new Node(config.withValue(Node.CONFIG_PARTITION_LIST, ConfigValueFactory.fromIterable(List(0, 1).asJava))) {
    startRegion(new Partition with GraphPartition)
  }

  gateway.awaitClusterReady()

  import gateway._

  override def afterAll(): Unit = {
    try {
      region.shutdown()
      gateway.shutdown()
    } finally {
      super.afterAll()
    }
  }

  "ExampleApp Gateway" should "be able to play ping pong" in {
    http_get(uri("/ping")).entity should be(jsonStringEntity("pong"))
  }

  "Admin requests" should "be authenticated with Basic Auth" in {
    val response1 = http_get(uri("/settings"))
    response1.status should be(Unauthorized)
    val authHeader = response1.header[WWWAuthenticate].get
    val challenge = authHeader.getChallenges.iterator().next
    challenge.realm() should be("Create admin password")
    challenge.scheme() should be("BASIC")
    http_get(uri("/settings"), List(Authorization.basic("admin", "1234"))).status should be(OK)
    http_get(uri("/settings"), List(Authorization.basic("admin", "wrong-password"))).status should be(Unauthorized)
  }

  "Public API requests" should "be allowed only with valid salted and time-based signature" in {
    val publicKey = "pkey1"
    val createApiKey = http_post(uri(s"/settings/add?key=$publicKey"), "", List(Authorization.basic("admin", "1234")))
    createApiKey.status should be(OK)
    implicit val materializer = ActorMaterializer.create(gateway.system)
    val salt = get_json(createApiKey).textValue
    val crypto = new TimeCryptoProofSHA256(salt)

    val requestUrl = uri("/profile/mypii")
    //unsigned request should be rejected
    val response1 = http_get(requestUrl)
    response1.status should be(Unauthorized)

    //signed request should have a valid response
    val requestSignature = crypto.sign(requestUrl.path.toString)
    val glue = if (requestUrl.rawQueryString.isDefined) "&" else "?"
    val signedRequestUrl = Uri(requestUrl + glue + "signature=" + publicKey + ":" + requestSignature)
    val response2 = http_get(signedRequestUrl)
    response2.status should be(OK)

    //the response should also be signed by the server and the response signature must be valid
    val json2 = Await.result(response2.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
    val jsonNode = mapper.readValue(json2, classOf[JsonNode])
    jsonNode.get("pii").textValue() should be("mypii")
    val responseSignature = jsonNode.get("signature").textValue()
    crypto.verify(responseSignature, requestSignature + "!")

  }

  "Graph API" should "should maintain connected components when adding and removing edges" in {
    //(1~>2), (3~>4) ==> component1(1,2), component3(3,4)
    http_get(uri("/vertex/1")).status should be(NotFound)
    http_get(uri("/vertex/2")).status should be(NotFound)
    http_post(uri("/connect/1/2")).status should be(SeeOther)
    http_post(uri("/connect/3/4")).status should be(SeeOther)
    get_json(http_get(uri("/vertex/1"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/2"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/3"))).get("data").get("component").intValue should be(3)
    get_json(http_get(uri("/vertex/4"))).get("data").get("component").intValue should be(3)
    get_json(http_get(uri("/component/1"))).get("data").get("connected").elements().asScala.map(_.intValue).toSet.diff(Set(1, 2)) should be(Set())
    http_get(uri("/component/2")).status should be(NotFound)
    get_json(http_get(uri("/component/3"))).get("data").get("connected").elements().asScala.map(_.intValue).toSet.diff(Set(3, 4)) should be(Set())
    http_get(uri("/component/4")).status should be(NotFound)

    //(3~>1)         ==> component1(1,2,3,4)
    http_post(uri("/connect/3/1")).status should be(SeeOther)
    get_json(http_get(uri("/vertex/1"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/2"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/3"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/4"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/component/1"))).get("data").get("connected").elements().asScala.map(_.intValue).toSet.diff(Set(1, 2, 3, 4)) should be(Set())
    http_get(uri("/component/2")).status should be(NotFound)
    http_get(uri("/component/3")).status should be(NotFound)
    http_get(uri("/component/4")).status should be(NotFound)

    //(4!>3)         ==> component1(1,2,3), component4(4)
    http_post(uri("/disconnect/4/3")).status should be(SeeOther)
    get_json(http_get(uri("/vertex/1"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/2"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/3"))).get("data").get("component").intValue should be(1)
    get_json(http_get(uri("/vertex/4"))).get("data").get("component").intValue should be(4)
    get_json(http_get(uri("/component/1"))).get("data").get("connected").elements().asScala.map(_.intValue).toSet.diff(Set(1, 2, 3)) should be(Set())
    http_get(uri("/component/2")).status should be(NotFound)
    http_get(uri("/component/3")).status should be(NotFound)
    get_json(http_get(uri("/component/4"))).get("data").get("connected").elements().asScala.map(_.intValue).toSet.diff(Set(4)) should be(Set())

  }


  "Graph API" should "stream changes to vertex websocket subscribers" in {
    val lastMessage = new AtomicReference[GenericRecord](null)
    lastMessage.synchronized {
      val ws = new AvroWebSocketClient(URI.create(s"ws://localhost:$httpPort/vertex?id=1000"), new AvroMessageHandler() {
        override def onMessage(message: scala.Any): Unit = {
          lastMessage.synchronized {
            lastMessage.set(message.asInstanceOf[GenericRecord])
            lastMessage.notify()
          }
        }
      })
      lastMessage.wait(1000)
      (lastMessage.get == null) should be(true)
      http_post(uri("/connect/1000/2000")).status should be(SeeOther)
      lastMessage.wait(1000)
      val msg = lastMessage.get
      (msg != null) should be(true)
      msg.getSchema.getName should be("VertexProps")
      msg.get("component") should be (1000)
      val edges = msg.get("edges").asInstanceOf[GenericData.Array[GenericRecord]]
      edges.size should be (1)
      edges.get(0).get("target") should be (2000)
      ws.close()
    }
  }

}
