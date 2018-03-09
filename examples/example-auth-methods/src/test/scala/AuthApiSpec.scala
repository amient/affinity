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

import akka.http.javadsl.model.headers._
import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.config.ConfigFactory
import io.amient.affinity.Conf
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.MemStoreSimpleMap
import io.amient.affinity.core.util.{AffinityTestBase, TimeCryptoProofSHA256}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.language.postfixOps

class AuthApiSpec extends FlatSpec with AffinityTestBase with Matchers with BeforeAndAfterAll {

  val config = ConfigFactory.parseMap(Map(
    Conf.Affi.Global("settings").MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
    Conf.Affi.Node.Gateway.Http.Host.path -> "127.0.0.1",
    Conf.Affi.Node.Gateway.Http.Port.path -> "0"
  ).asJava)

  val publicKey = "pkey1"

  val node1 = new Node(configure(config))

  lazy val crypto = new TimeCryptoProofSHA256({
    val createApiKey = node1.http_post(s"/settings/add?key=$publicKey", Array(), List(Authorization.basic("admin", "1234")))
    createApiKey.status should be(OK)
    node1.get_json(createApiKey).getTextValue
  })

  override def beforeAll(): Unit = try {
    node1.startGateway(new GatewayHttp with PrivateApi with PublicApi)
    node1.awaitClusterReady
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node1.shutdown()
  } finally {
    super.afterAll()
  }

  "Admin requests" should "be authenticated with Basic Auth" in {
    val response1 = node1.http_get("/settings")
    response1.status should be(Unauthorized)
    val authHeader = response1.header[WWWAuthenticate].get
    val challenge = authHeader.getChallenges.iterator().next
    challenge.realm() should be("Create admin password")
    challenge.scheme() should be("BASIC")
    node1.http_get("/settings", List(Authorization.basic("admin", "1234"))).status should be(OK)
    node1.http_get("/settings", List(Authorization.basic("admin", "wrong-password"))).status should be(Unauthorized)
  }

  "Public API requests" should "not be allowed without signature" in {
    //unsigned request should be rejected
    val response1 = node1.http_get("/profile/mypii")
    response1.status should be(Unauthorized)
  }

  "Public API requests" should "be allowed only with valid salted and time-based signature" in {
    //signed request should have a valid "/profile/mypii"
    val requestSignature = crypto.sign("/profile/mypii")
    val signedRequestUrl = "/profile/mypii?signature=" + publicKey + ":" + requestSignature
    val response = node1.http_get(signedRequestUrl)
    response.status should be(OK)
    //the response should also be signed by the server and the response signature must be valid
    val json = node1.get_json(response)
    json.get("profile").get("type").getTextValue should be ("ProtectedProfile")
    json.get("profile").get("data").get("hello").getTextValue should be("world")
    val responseSignature = json.get("signature").getTextValue
    crypto.verify(responseSignature, requestSignature + "!")
  }


}
