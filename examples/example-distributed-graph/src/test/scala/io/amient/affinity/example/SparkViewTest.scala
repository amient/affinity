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

package io.amient.affinity.spark

import java.util.Properties

import akka.http.scaladsl.model.StatusCodes.SeeOther
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.serde.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.core.serde.primitive.IntSerde
import io.amient.affinity.example.data.MyProjectAvroSerde
import io.amient.affinity.example.http.handler.{Admin, Graph, PublicApi}
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.rest.handler.Ping
import io.amient.affinity.kafka.KafkaFetcherImpl
import io.amient.affinity.model.graph.message.{Component, VertexProps}
import io.amient.affinity.testutil.SystemTestBaseWithKafka
import io.amient.util.spark.KafkaRDD
import org.apache.spark.serializer._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class MySparkAvroSerde(zk: String, to1: Int, to2: Int)
  extends ZkAvroSchemaRegistry(zk, to1, to2) with MyProjectAvroSerde


class ExampleGraphAnalyticsSystemTest extends FlatSpec with SystemTestBaseWithKafka with Matchers {

  override def numPartitions = 4

  val config = ConfigFactory.load("example")
    .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("ERROR"))


  val dataNode = new Node(configure(config))
  val gatewayNode = new TestGatewayNode(configure(config), new HttpGateway
    with Ping
    with Admin
    with PublicApi
    with Graph) {
    awaitClusterReady {
      dataNode.startContainer("graph", List(0, 1, 2, 3))
      dataNode.startContainer("user-mediator", List(0))
    }
  }

  import gatewayNode._

  http_post(uri("/connect/1/2")).status should be(SeeOther)
  http_post(uri("/connect/3/4")).status should be(SeeOther)
  http_post(uri("/connect/2/3")).status should be(SeeOther)

  override def afterAll(): Unit = {
    try {
      dataNode.shutdown()
      gatewayNode.shutdown()
    } finally {
      super.afterAll()
    }
  }

  "Spark Module" should "be able to see avro-serialised state in kafka" in {

    val _kafkaBootstrap = kafkaBootstrap
    val _zkConnect = zkConnect
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Affinity_Spark_2.0")
      .set("spark.serializer", classOf[KryoSerializer].getName)

    val sc = new SparkContext(conf)

    val graphFetcher = new KafkaFetcherImpl(topic = "graph", new Properties() {
      put("bootstrap.servers", _kafkaBootstrap)
    })

    val graphRdd = new KafkaRDD(sc, graphFetcher)
      .compact[Int, VertexProps](new IntSerde, new MySparkAvroSerde(_zkConnect, 6000, 6000))
      .repartition(1)
      .sortByKey()

    graphRdd.collect().toList match {
      case (1, VertexProps(_, 1, _)) ::
        (2, VertexProps(_, 1, _)) ::
        (3, VertexProps(_, 1, _)) ::
        (4, VertexProps(_, 1, _)) :: Nil =>
      case _ => throw new AssertionError("Graph should contain 4 vertices")
    }

    val componentFetcher = new KafkaFetcherImpl(topic = "components", new Properties() {
      put("bootstrap.servers", _kafkaBootstrap)
    })

    val componentRdd = new KafkaRDD(sc, componentFetcher)
      .compact[Int, Component](new IntSerde, new MySparkAvroSerde(_zkConnect, 6000, 6000))
      .repartition(1)

    componentRdd.collect.toList match {
      case (1, Component(_, _)) :: Nil =>
      case _ => throw new AssertionError("Graph should contain 1 component")
    }

  }

}

