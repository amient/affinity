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
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Actor.Receive
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import io.amient.affinity.core.actor.Gateway
import io.amient.affinity.core.cluster.{Cluster, Coordinator, CoordinatorZk, Node}
import io.amient.affinity.core.http.HttpInterface
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

trait SystemTestBase extends Suite with BeforeAndAfterAll {

  val testDir = new File(classOf[SystemTestBase].getResource("/systemtest").getPath())
  val numPartitions = 2
  deleteDirectory(testDir)

  //setup zookeeper
  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  zkFactory.startup(zookeeper)

  //setup kafka
  // TODO dynamic kafka port allocation
  val kafkaPort = "49092"
  val kafkaBootstrap = "localhost:" + kafkaPort
  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    {
      put("broker.id", "1")
      put("host.name", "localhost")
      put("port", kafkaPort)
      put("log.dir", embeddedKafkaPath.toString)
      put("num.partitions", numPartitions.toString)
      put("auto.create.topics.enable", "true")
      put("zookeeper.connect", zkConnect)
    }
  })
  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  override def afterAll(): Unit = {
    try {
      //simpleProducer.close()
      System.out.println("Shutting down kafka...")
      try {
        kafka.shutdown()
      } catch {
        case e: IllegalStateException => //
      }
      System.out.println("Shutting down zookeeper...")
      zkFactory.shutdown()
    } finally {
      System.out.println("Cleaning up directories...")
      deleteDirectory(embeddedZkPath)
      deleteDirectory(embeddedKafkaPath)
    }
  }

  private def deleteDirectory(path: File) = {
    def getRecursively(f: File): Seq[File] =
      f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
    getRecursively(path).foreach { f =>
      if (!f.delete())
        throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
    }
  }

  def createGatewayNode[T <: Gateway](httpPort: Int, akkaPort: Int)(handler: Receive)
                                     (implicit tag: ClassTag[T]): Node = {
    val affinityConfig = new Properties()
    affinityConfig.put(HttpInterface.CONFIG_HTTP_HOST, "localhost")
    affinityConfig.put(HttpInterface.CONFIG_HTTP_PORT, httpPort.toString)
    affinityConfig.put(Cluster.CONFIG_NUM_PARTITIONS, numPartitions.toString)
    affinityConfig.put(Node.CONFIG_AKKA_SYSTEM, "SystemTest")
    affinityConfig.put(Node.CONFIG_AKKA_HOST, "localhost")
    affinityConfig.put(Node.CONFIG_AKKA_PORT, akkaPort.toString)
    affinityConfig.put(Node.CONFIG_AKKA_CONF_NAME, "systemtests")
//    affinityConfig.put(Region.CONFIG_PARTITION_LIST, "0")
    affinityConfig.put(Coordinator.CONFIG_COORDINATOR_CLASS, classOf[CoordinatorZk].getName)
    affinityConfig.put(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, zkConnect)

    val startupMonitor = new AtomicBoolean(false)

    val node = new Node(affinityConfig) {
      startGateway {
        new Gateway(affinityConfig) {
          override def preStart(): Unit = {
            super.preStart()
            startupMonitor.synchronized {
              startupMonitor.set(true)
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

    val timeout:Duration = (5 seconds)
    startupMonitor.synchronized(startupMonitor.wait(timeout.toMillis))
    if (!startupMonitor.get) {
      throw new IllegalStateException(s"Node did not startup within $timeout")
    } else {
      return node
    }
  }

  def createProducer() = new KafkaProducer[String, String](Map[String, AnyRef](
    "metadata.broker.list" -> kafkaBootstrap,
    "serializer.class" -> "kafka.serializer.StringEncoder",
    "request.required.acks" -> "1"
  ).asJava)


  //  def readFullyAsString(file: File , maxSize: Int) {
  //      try (FSDataInputStream in = localFileSystem.open(file)) {
  //        byte[] bytes = new byte[Math.min(in.available(), maxSize)];
  //        in.readFully(bytes);
  //        return new String(bytes);
  //      }
  //    }
}
