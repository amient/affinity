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

package io.amient.affinity.systemtests

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{Gateway, Partition}
import io.amient.affinity.core.cluster.{Cluster, CoordinatorZk, Node}
import io.amient.affinity.core.serde.StringSerde
import io.amient.affinity.core.util.{ObjectHashPartitioner, ZooKeeperClient}
import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

object SystemTestBase {
  val akkaPort = new AtomicInteger(15001)
}

trait SystemTestBase extends Suite with BeforeAndAfterAll {

  val testDir = new File(classOf[SystemTestBase].getResource("/systemtest").getPath() + "/" + this.getClass.getSimpleName)
  deleteDirectory(testDir)
  testDir.mkdirs()

  private def deleteDirectory(path: File) = if (path.exists()) {
    def getRecursively(f: File): Seq[File] = f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
    getRecursively(path).foreach(f => if (!f.delete()) throw new RuntimeException("Failed to delete " + f.getAbsolutePath))
  }

  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  zkFactory.startup(zookeeper)


  val config = ConfigFactory.load("systemtests")
    .withValue(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
    .withValue(Gateway.CONFIG_HTTP_PORT, ConfigValueFactory.fromAnyRef(0))

  val gatewayStartupMonitor = new AtomicInteger(-1)
  val regionStartupMonitor = new AtomicInteger(config.getInt(Cluster.CONFIG_NUM_PARTITIONS))

  import SystemTestBase._

  def awaitRegions() = {
    val timeout: Duration = (5 seconds)
    regionStartupMonitor.synchronized {
      while (regionStartupMonitor.getAndDecrement() > 0) {
        regionStartupMonitor.wait(timeout.toMillis)
      }
    }
  }

  override def afterAll(): Unit = {
    zkFactory.shutdown()
  }

  def jsonStringEntity(s: String) = HttpEntity.Strict(ContentTypes.`application/json`, ByteString("\"" + s + "\""))

  class TestGateway extends Gateway {

    override def preStart(): Unit = {
      super.preStart()
      gatewayStartupMonitor.synchronized {
        gatewayStartupMonitor.set(getHttpPort)
        gatewayStartupMonitor.notify
      }
    }

    override def handleException: PartialFunction[Throwable, HttpResponse] = {
      case e: NoSuchElementException => HttpResponse(NotFound)
      case e: Throwable => e.printStackTrace(); HttpResponse(InternalServerError)
    }

  }

  class TestGatewayNode(gateway: => TestGateway)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(0))) {

    import system.dispatcher

    implicit val materializer = ActorMaterializer.create(system)

    val timeout: Duration = (5 seconds)
    val httpPort = gatewayStartupMonitor.synchronized {
      startGateway(gateway)
      gatewayStartupMonitor.wait(timeout.toMillis)
      gatewayStartupMonitor.get
    }

    if (httpPort <= 0) {
      throw new IllegalStateException(s"Node did not startup within $timeout")
    }

    def http(method: HttpMethod, path: String): HttpResponse = {
      val uri = Uri(s"http://localhost:$httpPort$path")
      val strict = Http().singleRequest(HttpRequest(method = method, uri = uri)) flatMap {
        response => response.toStrict(2 seconds)
      }
      Await.result(strict, 2 seconds)
    }

  }

  class TestRegionNode(partitionCreator: => Partition)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(akkaPort.getAndIncrement()))) {
    startRegion(partitionCreator)
  }

  abstract class TestPartition extends Partition {

    override def onBecomeMaster: Unit = {
      super.onBecomeMaster
      regionStartupMonitor.synchronized(regionStartupMonitor.notify)
    }

  }

}


trait SystemTestBaseWithKafka extends SystemTestBase {

  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    {
      put("broker.id", "1")
      put("host.name", "localhost")
      put("port", "0")
      put("log.dir", embeddedKafkaPath.toString)
      put("num.partitions", "2")
      put("auto.create.topics.enable", "true")
      put("zookeeper.connect", zkConnect)
    }
  })
  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  val tmpZkClient = new ZooKeeperClient(zkConnect)
  val broker = Broker.createBroker(1, tmpZkClient.readData[String]("/brokers/ids/1"))
  val kafkaBootstrap = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).connectionString()
  tmpZkClient.close

  override def afterAll(): Unit = {
    try {
      kafka.shutdown()
    } catch {
      case e: IllegalStateException => //
    }
    super.afterAll()
  }

  def createProducer() = new KafkaProducer[String, String](Map[String, AnyRef](
    "bootstrap.servers" -> kafkaBootstrap,
    "key.serializer" -> classOf[StringSerde].getName,
    "value.serializer" -> classOf[StringSerde].getName,
    "partitioner.class" -> classOf[ObjectHashPartitioner].getName,
    "acks" -> "all"
  ).asJava)
}
