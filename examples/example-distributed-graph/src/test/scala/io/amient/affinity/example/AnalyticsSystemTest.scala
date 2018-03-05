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

import akka.http.scaladsl.model.StatusCodes.SeeOther
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.LogStorage
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.example.graph.message.{Component, VertexProps}
import io.amient.affinity.example.http.handler.{Admin, Graph, PublicApi}
import io.amient.affinity.example.rest.ExampleGatewayRoot
import io.amient.affinity.example.rest.handler.Ping
import io.amient.affinity.kafka.EmbeddedKafka
import io.amient.affinity.spark.LogRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

class AnalyticsSystemTest extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {

  override def numPartitions = 4

  val config: Config = ConfigFactory.load("example")
    .withValue(Conf.Affi.Node.Gateway.Http.Host.path, ConfigValueFactory.fromAnyRef("127.0.0.1"))
    .withValue(Conf.Affi.Node.Gateway.Http.Port.path, ConfigValueFactory.fromAnyRef(0))

  val node2 = new Node(configure(config, Some(zkConnect), Some(kafkaBootstrap)))
  val node1 = new Node(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

  override def beforeAll(): Unit = try {
    node1.startGateway(new ExampleGatewayRoot with Ping with Admin with PublicApi with Graph)
    node2.startContainer("graph", List(0, 1, 2, 3))
    node1.awaitClusterReady
    node1.http_post("/connect/1/2").status should be(SeeOther)
    node1.http_post("/connect/3/4").status should be(SeeOther)
    node1.http_post("/connect/2/3").status should be(SeeOther)
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node2.shutdown()
    node1.shutdown()
  } finally {
    super.afterAll()
  }

  "Spark Module" should "be able to see avro-serialised state in kafka" in {

    node1.get_json(node1.http_get("/vertex/1")).get("data").get("component").getIntValue should be(1)
    node1.get_json(node1.http_get("/vertex/4")).get("data").get("component").getIntValue should be(1)

    implicit val sc = new SparkContext(new SparkConf()
      .setMaster("local[4]")
      .set("spark.driver.host", "localhost")
      .setAppName("Affinity_Spark")
      .set("spark.serializer", classOf[KryoSerializer].getName))

    implicit val conf = Conf(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

    val graphRdd = SparkDriver.graphRdd
    val sortedGraph = graphRdd.repartition(1).sortByKey().collect().toList
    sortedGraph match {
      case (1, VertexProps(_, 1, _)) ::
        (2, VertexProps(_, 1, _)) ::
        (3, VertexProps(_, 1, _)) ::
        (4, VertexProps(_, 1, _)) :: Nil =>
      case x =>
        throw new AssertionError(s"Graph should contain 4 vertices but was: $x")
    }


    SparkDriver.componentRdd.collect.toList match {
      case (1, Component(_, _)) :: Nil =>
      case x => throw new AssertionError(s"Graph should contain 1 component, got: $x")
    }

    val updateBatch: RDD[(Int, Component)] = sc.parallelize(Array((1, null), (2, Component(0L, Set()))))
    SparkDriver.avroUpdate("graph", "components", updateBatch)

    SparkDriver.componentRdd.collect.toList match {
      case (2, Component(0L, _)) :: Nil =>
      case other => throw new AssertionError("Graph should contain 1 component")
    }

  }

}

object SparkDriver {

  def graphRdd(implicit conf: Conf, sc: SparkContext) = avroRdd[Int, VertexProps]("graph", "graph")

  def componentRdd(implicit conf: Conf, sc: SparkContext) = avroRdd[Int, Component]("graph", "components")

  def avroRdd[K: ClassTag, V: ClassTag](ks: String, store: String)(implicit conf: Conf, sc: SparkContext) = {
    val avroConf = conf.Affi.Avro
    val storageConf = conf.Affi.Keyspace(ks).State(store).Storage
    new LogRDD(sc, LogStorage.newInstance(storageConf)).compact.present[K, V](AvroSerde.create(avroConf))
  }

  def avroUpdate[K: ClassTag, V: ClassTag](ks: String, store: String, data: RDD[(K, V)])(implicit conf: Conf, sc: SparkContext): Unit = {
    val avroConf = conf.Affi.Avro
    val storageConf = conf.Affi.Keyspace(ks).State(store).Storage
    LogRDD.append(AvroSerde.create(avroConf), LogStorage.newInstance(storageConf), data)
  }
}
