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

package io.amient.affinity.testutil

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{Cluster, Gateway, Partition}
import io.amient.affinity.core.cluster.{CoordinatorZk, Node}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object SystemTestBase {
  val akkaPort = new AtomicInteger(15001)
}

trait SystemTestBase extends Suite with BeforeAndAfterAll {

  val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  println(s"Test dir: $testDir")
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

  import SystemTestBase._

  override def afterAll(): Unit = {
    zkFactory.shutdown()
  }

  def jsonStringEntity(s: String) = HttpEntity.Strict(ContentTypes.`application/json`, ByteString("\"" + s + "\""))

  class TestGatewayNode(gateway: => Gateway)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(0))) {

    import system.dispatcher

    implicit val materializer = ActorMaterializer.create(system)

    val timeout: Duration = (5 seconds)
    val httpPort: Int = Await.result(startGateway(gateway), 10 seconds)

    if (httpPort <= 0) {
      throw new IllegalStateException(s"Node did not startup within $timeout")
    } else {
      println(s"TestGatewayNode listening on $httpPort")
    }

    def http(method: HttpMethod, path: String): Future[HttpResponse] = {
      val uri = Uri(s"http://localhost:$httpPort$path")
      Http().singleRequest(HttpRequest(method = method, uri = uri)) flatMap {
        response =>
          //println(response.headers)
          response.toStrict(2 seconds)
      }
    }

    def http_sync(method: HttpMethod, path: String): HttpResponse = {
      Await.result(http(method, path), 2 seconds)
    }

  }

  class TestRegionNode(partitionCreator: => Partition)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(akkaPort.getAndIncrement()))) {
    Await.result(startRegion(partitionCreator), 3 seconds)
  }

}


