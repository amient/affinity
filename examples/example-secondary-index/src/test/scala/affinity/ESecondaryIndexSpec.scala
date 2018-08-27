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

package affinity

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Properties, UUID}

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Random

class ESecondaryIndexSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with BeforeAndAfterAll with Matchers {
  override def numPartitions = 3

  val MemorySchemaRegistryId = "39475"

  val testNodeDataDir = createTempDirectory
  val config = ConfigFactory.parseMap(Map(
    "ZOOKEEPER_CONNECT" -> zkConnect,
    "KAFKA_CONNECTION" -> kafkaBootstrap,
    "affinity.node.data.dir" -> testNodeDataDir.toString,
    "affinity.avro.schema.registry.class" -> classOf[MemorySchemaRegistry].getName,
    "affinity.avro.schema.registry.id" -> MemorySchemaRegistryId
  ).asJava).withFallback(ConfigFactory.parseResources("example.conf")).resolve()

  val node = new Node(config)

  val inputTopic = config.getString("affinity.node.gateway.stream.input-stream.kafka.topic")

  val producerProps = new Properties() {

    import ProducerConfig._

    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(ACKS_CONFIG, "1")
    put(KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
    put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
    put("schema.registry.id", MemorySchemaRegistryId)
  }

  //08am 26 June 2018
  val a1 = Article(title = "Green Buildings", timestamp = 1530000000000L)
  val a2 = Article(title = "Liquid-Cooled Microchips", timestamp = 1530000000000L) //08am 26 June 2018
  val a3 = Article(title = "Green Self-Driving Cars", timestamp = 1530086400000L) //08am 27 June 2018
  val a4 = Article(title = "Quantum Machines", timestamp = 1530090000000L) //09am 27 June 2018
  val a5 = Article(title = "Green materials", timestamp = 1530172800000L) //08am 28 June 2018
  val a6 = Article(title = "Green CVT Cars", timestamp = 1530172800000L) //08am 28 June 2018


  override def beforeAll(): Unit = {
    createTopic(inputTopic)

    val producer = new KafkaProducer[Author, Article](producerProps)
    try {
      val randomPartition = new Random()

      def produceTestArticle(author: Author, article: Article): Unit = {
        producer.send(new ProducerRecord(inputTopic, randomPartition.nextInt(numPartitions), article.timestamp, author, article))
      }

      produceTestArticle(Author("mtjames"), a1)
      produceTestArticle(Author("cowboy"), a2)
      produceTestArticle(Author("mtjames"), a3)
      produceTestArticle(Author("cowboy"), a4)
      produceTestArticle(Author("sandra81"), a5)
      produceTestArticle(Author("sandra81"), a6)
      producer.flush()
    } finally {
      producer.close()
    }
    node.start()
    node.awaitClusterReady()

    //make sure that before starting the suite tests all the produced fixtures have been processed
    val latch = new CountDownLatch(6)
    node.system.eventStream.subscribe(
      node.system.actorOf(Props(new Actor {
        override def receive = {
          case _ => latch.countDown()
        }
      })), classOf[StoreArticle])
    latch.await(10, TimeUnit.SECONDS)
  }

  override def afterAll(): Unit = {
    try {
      node.shutdown()
    } finally {
      deleteDirectory(testNodeDataDir)
    }
  }


  "Articles Store" should "see all authors artiles" in {
    val articles = node.get_json(node.http_get("/articles/mtjames")).getElements.asScala
    articles.size should be(2)
  }

  "Articles Store" should "see all articles conatining a word" in {
    val words = node.get_json(node.http_get("/words/green")).getElements.asScala.toList
    words.foreach(println)
    words.size should be(4)
  }

  "Articles Store" should "see all articles conatining a word limited by timerange" in {
    val wordsSince = node.get_json(node.http_get("/words-since/green")).getElements.asScala.toList
    wordsSince.size should be(3)
  }

  "Articles Store" should "deindex removed articles entries" in {
    node.get_json(node.http_get("/words/machines")).getElements.asScala.toList.size should be(1)
    node.http_get("/delete-articles-containing/machines").status should be (StatusCodes.Accepted)
    node.get_json(node.http_get("/words/quantum")).getElements.asScala.toList.size should be (0)
    node.get_json(node.http_get("/words/machines")).getElements.asScala.toList.size should be (0)
  }
}
