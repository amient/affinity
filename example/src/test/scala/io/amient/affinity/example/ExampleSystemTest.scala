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

import akka.http.scaladsl.model.HttpMethods._
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.rest.handler.Ping
import io.amient.affinity.testutil.SystemTestBaseWithKafka
import org.scalatest.{FlatSpec, Matchers}

class ExampleSystemTest extends FlatSpec with SystemTestBaseWithKafka with Matchers {

  val config = configure("example")
      .withValue(Node.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
      .withValue(KafkaStorage.CONFIG_KAFKA_BOOTSTRAP_SERVERS("settings"), ConfigValueFactory.fromAnyRef(kafkaBootstrap))
      .withValue(KafkaStorage.CONFIG_KAFKA_BOOTSTRAP_SERVERS("graph"), ConfigValueFactory.fromAnyRef(kafkaBootstrap))

  val gateway = new TestGatewayNode(config, new HttpGateway with Ping)

  "ExampleApp Gateway" should "be able to play ping pong" in {
    gateway.http_sync(GET, s"/ping").entity should be(jsonStringEntity("pong"))
  }
}
