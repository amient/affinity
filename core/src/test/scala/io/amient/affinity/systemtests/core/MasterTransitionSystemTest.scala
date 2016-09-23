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

import akka.http.scaladsl.model.{ContentTypes, HttpResponse, Uri, headers}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Path._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http.ResponseBuilder
import io.amient.affinity.core.serde.primitive.StringSerde
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreSimpleMap}
import io.amient.affinity.systemtests.SystemTestBaseWithKafka
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class MasterTransitionSystemTest extends FlatSpec with SystemTestBaseWithKafka with Matchers {
  val topic = "test"

  val producer = createProducer()
  producer.send(new ProducerRecord[String,String](topic, "A", "initialValueA"))
  producer.send(new ProducerRecord[String,String](topic, "B", "initialValueB"))
  producer.close()

  val gateway = new TestGatewayNode(new TestGateway {

    import context.dispatcher

    override def handle: Receive = super.handle orElse {
      case HTTP(GET, PATH(key), _, response) =>
        implicit val timeout = Timeout(1 second)
        fulfillAndHandleErrors(response, cluster ? key, ContentTypes.`application/json`) {
          case value => ResponseBuilder.json(OK, value, gzip = false)
        }

      case HTTP(POST, PATH(key, value), _, response) =>
        implicit val timeout = Timeout(1 second)
        fulfillAndHandleErrors(response, ack(cluster, (key, value)), ContentTypes.`application/json`) {
          case result => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/$key"))))
        }

      case HTTP(GET, SingleSlash, _, response) =>
        response.success(ResponseBuilder.json(OK, Map(
          "singleton-services" -> describeServices,
          "partition-masters" -> describeRegions
        ), gzip = false))
    }
  })

  class MyTestPartition extends TestPartition {
    val data = storage {
      new KafkaStorage[String, String](kafkaBootstrap, topic, partition, classOf[StringSerde], classOf[StringSerde])
        with MemStoreSimpleMap[String, String]
    }

    override def handle: Receive = {
      case key: String => sender ! data.get(key)
      case (key: String, value: String) => ack(sender) {
        data.put(key, Some(value))
      }
    }
  }

  val region1 = new TestRegionNode(new MyTestPartition)
  val region2 = new TestRegionNode(new MyTestPartition)

  awaitRegions()

  override def afterAll(): Unit = {
    try {
      gateway.shutdown()
      region1.shutdown()
      region2.shutdown()
    } finally {
      super.afterAll()
    }
  }

  "Master" should "have all data after startup" in {
    gateway.http(GET, "/A").entity should be (jsonStringEntity("initialValueA"))
    gateway.http(GET, "/B").entity should be (jsonStringEntity("initialValueB"))
    gateway.http(POST, "/A/updatedValueA").status.intValue should be (303)
    gateway.http(GET, "/A").entity should be (jsonStringEntity("updatedValueA"))
  }

  //TODO #6 "Master Transition" should "not cause requests being dropped" in {}

  //TODO #6 "Master Transition" should "not lead to inconsistent state" in {}

}
