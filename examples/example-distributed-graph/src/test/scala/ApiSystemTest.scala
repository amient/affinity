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

import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import io.amient.affinity.ws.WebSocketClient
import io.amient.affinity.ws.WebSocketClient.AvroMessageHandler
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.language.postfixOps

class ApiSystemTest extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {

  override def numPartitions = 2

  val config = ConfigFactory.load("example")
    .withValue(Conf.Affi.Keyspace("graph").NumPartitions.path, ConfigValueFactory.fromAnyRef(numPartitions))
    .withValue(Conf.Affi.Node.Gateway.Http.Host.path, ConfigValueFactory.fromAnyRef("127.0.0.1"))
    .withValue(Conf.Affi.Node.Gateway.Http.Port.path, ConfigValueFactory.fromAnyRef(0))


  val nodeConfig = configure(config, Some(zkConnect), Some(kafkaBootstrap))
  val node2 = new Node(nodeConfig)
  val node1 = new Node(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

  override def beforeAll(): Unit = try {
    node1.startGateway(new ExampleGateway)
    node2.startContainer("graph", List(0, 1))
    node1.awaitClusterReady
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node2.shutdown()
    node1.shutdown()
  } finally {
    super.afterAll()
  }

  "Graph API" should "should maintain connected components when adding and removing edges" in {
    //(1~>2), (3~>4) ==> component1(1,2), component3(3,4)
    node1.http_get("/vertex/1").status should be(NotFound)
    node1.http_get("/vertex/2").status should be(NotFound)
    node1.http_post("/connect/1/2").status should be(SeeOther)
    node1.http_post("/connect/3/4").status should be(SeeOther)
    node1.get_json(node1.http_get("/vertex/1")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/2")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/3")).get("data").get("component").getIntValue should be(3)
    node1.get_json(node1.http_get("/vertex/4")).get("data").get("component").getIntValue should be(3)
    node1.get_json(node1.http_get("/component/1")).get("data").get("connected").getElements().asScala.map(_.getIntValue).toSet.diff(Set(1, 2)) should be(Set())
    node1.http_get("/component/2").status should be(NotFound)
    node1.get_json(node1.http_get("/component/3")).get("data").get("connected").getElements().asScala.map(_.getIntValue).toSet.diff(Set(3, 4)) should be(Set())
    node1.http_get("/component/4").status should be(NotFound)

    //(3~>1)         ==> component1(1,2,3,4)
    node1.http_post("/connect/3/1").status should be(SeeOther)
    node1.get_json(node1.http_get("/vertex/1")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/2")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/3")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/4")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/component/1")).get("data").get("connected").getElements().asScala.map(_.getIntValue).toSet.diff(Set(1, 2, 3, 4)) should be(Set())
    node1.http_get("/component/2").status should be(NotFound)
    node1.http_get("/component/3").status should be(NotFound)
    node1.http_get("/component/4").status should be(NotFound)

    //(4!>3)         ==> component1(1,2,3), component4(4)
    node1.http_post("/disconnect/4/3").status should be(SeeOther)
    node1.get_json(node1.http_get("/vertex/1")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/2")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/3")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/4")).get("data").get("component").getIntValue should be(4)
    node1.get_json(node1.http_get("/component/1")).get("data").get("connected").getElements().asScala.map(_.getIntValue).toSet.diff(Set(1, 2, 3)) should be(Set())
    node1.http_get("/component/2").status should be(NotFound)
    node1.http_get("/component/3").status should be(NotFound)
    node1.get_json(node1.http_get("/component/4")).get("data").get("connected").getElements().asScala.map(_.getIntValue).toSet.diff(Set(4)) should be(Set())

  }


  "Graph API" should "stream changes to vertex websocket subscribers" in {
    val lastMessage = new AtomicReference[GenericRecord](null)
    lastMessage.synchronized {
      val ws = new WebSocketClient(node1.wsuri("/vertex?id=1000"), new AvroMessageHandler() {
        override def onError(e: Throwable): Unit = e.printStackTrace()

        override def onMessage(message: scala.Any): Unit = {
          lastMessage.synchronized {
            lastMessage.set(message.asInstanceOf[GenericRecord])
            lastMessage.notify()
          }
        }
      })
      lastMessage.wait(5000)
      (lastMessage.get == null) should be(true)
      node1.http_post("/connect/1000/2000").status should be(SeeOther)
      lastMessage.wait(5000)
      val msg = lastMessage.get
      (msg != null) should be(true)
      msg.getSchema.getName should be("VertexProps")
      msg.get("component") should be(1000)
      val edges = msg.get("edges").asInstanceOf[GenericData.Array[GenericRecord]]
      edges.size should be(1)
      edges.get(0).get("target") should be(2000)
      ws.close()
    }
  }

}
