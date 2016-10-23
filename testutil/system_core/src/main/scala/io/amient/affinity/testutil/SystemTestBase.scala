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
import java.util.zip.GZIPInputStream

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.StreamConverters._
import akka.util.ByteString
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{Cluster, Gateway, Partition}
import io.amient.affinity.core.cluster.{CoordinatorZk, Node}
import org.apache.avro.util.ByteBufferInputStream
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object SystemTestBase {
  val akkaPort = new AtomicInteger(15001)
}

trait SystemTestBase extends Suite with BeforeAndAfterAll {

  val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  println(s"Test dir: $testDir")
  testDir.mkdirs()

  private def deleteDirectory(path: File) = if (path.exists()) {
    def getRecursively(f: File): Seq[File] = f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
    getRecursively(path).foreach(f => if (!f.delete()) throw new RuntimeException("Failed to delete " + f.getAbsolutePath))
  }

  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  // smaller testDir footprint, default zookeeper file blocks are 65535Kb
  System.getProperties().setProperty("zookeeper.preAllocSize", "64")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  zkFactory.startup(zookeeper)

  final def configure(): Config = configure(ConfigFactory.defaultReference())

  final def configure(confname: String): Config = configure(ConfigFactory.load(confname)
    .withFallback(ConfigFactory.defaultReference()))

  def configure(config: Config): Config = config
    .withValue(Cluster.CONFIG_NUM_PARTITIONS, ConfigValueFactory.fromAnyRef(2))
    .withValue(Node.CONFIG_AKKA_STARTUP_TIMEOUT_MS, ConfigValueFactory.fromAnyRef(15000))
    .withValue(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
    .withValue(Gateway.CONFIG_HTTP_PORT, ConfigValueFactory.fromAnyRef(0))

  import SystemTestBase._

  override def afterAll(): Unit = {
    try {
      zkFactory.shutdown()
    } finally {
      deleteDirectory(testDir)
    }
  }

  def jsonStringEntity(s: String) = HttpEntity.Strict(ContentTypes.`application/json`, ByteString("\"" + s + "\""))

  class TestGatewayNode(config: Config, gateway: => Gateway)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(0))) {

    import system.dispatcher

    implicit val materializer = ActorMaterializer.create(system)

    val httpPort: Int = Await.result(startGateway(gateway), startupTimeout)

    if (httpPort <= 0) {
      throw new IllegalStateException(s"Gateway node failed to start")
    } else {
      println(s"TestGatewayNode listening on $httpPort")
    }

    def uri(path: String) = Uri(s"http://localhost:$httpPort$path")

    def http(method: HttpMethod, uri: Uri): Future[HttpResponse] = {
      http(HttpRequest(method = method, uri = uri))
    }

    def http_get(uri: Uri, headers: List[HttpHeader] = List()): HttpResponse = {
      Await.result(http(HttpRequest(method = HttpMethods.GET, uri = uri, headers = headers)), 2 seconds)
    }

    def http_get(uri: Uri): HttpResponse = {
      Await.result(http(HttpRequest(method = HttpMethods.GET, uri = uri)), 5 seconds)
    }

    val mapper = new ObjectMapper()
    def get_json(response: HttpResponse): JsonNode = {
      val json = Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
      mapper.readValue(json, classOf[JsonNode])
    }


    def http_post(uri: Uri, entity: String = "", headers: List[HttpHeader] = List()): HttpResponse = {
      Await.result(http(HttpRequest(method = HttpMethods.POST, uri = uri, headers = headers)), 2 seconds)
    }

    def http(req: HttpRequest) = {
      val decodedResponse: Future[HttpResponse] = Http().singleRequest(req) flatMap {
        response => response.header[headers.`Content-Encoding`] match {
          case Some(c) if (c.encodings.contains(HttpEncodings.gzip)) =>
            response.entity.dataBytes.map(_.asByteBuffer).runWith(Sink.seq).map {
              byteBufferSequence =>
                val unzipped = fromInputStream(() => new GZIPInputStream(new ByteBufferInputStream(byteBufferSequence.asJava)))
                val unzippedEntity =  HttpEntity(response.entity.contentType, unzipped)
                response.copy(entity = unzippedEntity)
            }
          case _ => Future.successful(response)
        }
      }
      decodedResponse.flatMap(_.toStrict(2 seconds))
    }

  }

  class TestRegionNode(config: Config, partitionCreator: => Partition)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(akkaPort.getAndIncrement()))) {
    Await.result(startRegion(partitionCreator), 36 seconds)
  }

}


