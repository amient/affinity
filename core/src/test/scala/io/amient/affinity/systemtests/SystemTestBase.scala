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

import akka.actor.Actor.Receive
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.Gateway
import io.amient.affinity.core.cluster.{CoordinatorZk, Node}
import io.amient.affinity.core.util.ZooKeeperClient
import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

trait SystemTestBase extends Suite with BeforeAndAfterAll {

  val testDir = new File(classOf[SystemTestBase].getResource("/systemtest").getPath())
  deleteDirectory(testDir)

  //setup zookeeper
  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  zkFactory.startup(zookeeper)

  //setup kafka

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
    zkFactory.shutdown()
  }

  private def deleteDirectory(path: File) = {
    def getRecursively(f: File): Seq[File] =
      f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
    getRecursively(path).foreach { f =>
      if (!f.delete())
        throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
    }
  }

  /**
    * @param handler
    * @param tag
    * @tparam T
    * @return (created Node instance, listening http port number)
    */
  def createGatewayNode[T <: Gateway]()(handler: Receive)(implicit tag: ClassTag[T]): (Node, Int) = {

    val config = ConfigFactory.load("systemtests")
      .withValue(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
      .withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(0))
      .withValue(Gateway.CONFIG_HTTP_PORT, ConfigValueFactory.fromAnyRef(0))

    val startupMonitor = new AtomicInteger(-1)

    val node = new Node(config) {
      startGateway {
        new Gateway {
          override def preStart(): Unit = {
            super.preStart()
            startupMonitor.synchronized {
              startupMonitor.set(getHttpPort)
              startupMonitor.notify
            }
          }

          override def handleException: PartialFunction[Throwable, HttpResponse] = {
            case e: NoSuchElementException => HttpResponse(NotFound)
            case e: Throwable => e.printStackTrace(); HttpResponse(InternalServerError)
          }

          override def handle: Receive = handler
        }
      }
    }

    val timeout: Duration = (5 seconds)
    startupMonitor.synchronized(startupMonitor.wait(timeout.toMillis))
    if (startupMonitor.get <= 0) {
      throw new IllegalStateException(s"Node did not startup within $timeout")
    } else {
      return (node, startupMonitor.get)
    }
  }

  def createProducer() = new KafkaProducer[String, String](Map[String, AnyRef](
    "metadata.broker.list" -> kafkaBootstrap,
    "serializer.class" -> "kafka.serializer.StringEncoder",
    "request.required.acks" -> "1"
  ).asJava)

}
