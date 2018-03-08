/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.kafka

import java.util

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import io.amient.affinity.AffinityActorSystem
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.Murmur2Partitioner
import org.apache.kafka.common.{Cluster, Node, PartitionInfo}
import org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class KafkaPartitionerSpec extends FlatSpec with Matchers {

  def mockCluster(numParts: Int) = new Cluster("mock-cluster",
    util.Arrays.asList[Node](),
    (0 to numParts - 1).map(p => new PartitionInfo("test", p, null, Array(), Array())),
    new util.HashSet[String],
    new util.HashSet[String])

  "kafka.DefaultPartitioner" should "have identical method to Murmur2Partitioner" in {
    val kafkaPartitioner = new org.apache.kafka.clients.producer.internals.DefaultPartitioner()
    val affinityPartitioner = new Murmur2Partitioner
    val key = "test-value-for-partitioner"
    val serializedKey: Array[Byte] = key.getBytes
    val kafkaP = kafkaPartitioner.partition("test", key, serializedKey, key, serializedKey, mockCluster(4))
    val affinityP = affinityPartitioner.partition(serializedKey, 4)
    kafkaP should equal(affinityP)
  }

  "KafkaAvroSerde" should "have identical serialization footprint as Akka AvroSerdeProxy" in {
    val cfg = Map(
      "schema.registry.class" -> classOf[MemorySchemaRegistry].getName,
      "schema.registry.id" -> "1"
    )

    val key = "6290853012217500191217"

    val system = AffinityActorSystem.create("test",
      ConfigFactory.parseMap(cfg.map { case (k, v) => ("affinity.avro." + k, v) }))
    val akkaSerializedKey = try {
      val serialization = SerializationExtension(system)
      serialization.serialize(key).get
    } finally {
      system.terminate()
    }
    val kafkaSerde = new KafkaAvroSerde()
    kafkaSerde.configure(cfg, true)
    val kafkaSerialized = kafkaSerde.serializer().serialize("test", key)
    akkaSerializedKey.mkString(".") should equal(kafkaSerialized.mkString("."))
    new Murmur2Partitioner().partition(akkaSerializedKey, 9) should be(4)
    new Murmur2Partitioner().partition(kafkaSerialized, 9) should be(4)
    val streamsPartitioner = new DefaultStreamPartitioner[Any, Any](kafkaSerde.serializer(), mockCluster(9), "test")
    streamsPartitioner.partition(key, null, 9) should be(4)
    streamsPartitioner.partition(key, "irrelevant", 9) should be(4)
  }

}
