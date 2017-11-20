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
import java.security.cert.CertificateFactory
import java.security.{KeyStore, SecureRandom}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.zip.GZIPInputStream
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.StreamConverters._
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.ServicesApi.GatewayClusterStatus
import io.amient.affinity.core.actor.{GatewayHttp, ServicesApi}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.Encoder
import org.apache.avro.util.ByteBufferInputStream
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object SystemTestBase {
  val akkaPort = new AtomicInteger(15001)
}

trait SystemTestBase extends Suite with BeforeAndAfterAll {

  final def configure(): Config = configure(ConfigFactory.defaultReference())

  final def configure(confname: String): Config = configure(ConfigFactory.load(confname)
    .withFallback(ConfigFactory.defaultReference()))

  def configure(config: Config): Config = config
    .withValue(Node.CONFIG_NODE_SYSTEM_NAME, ConfigValueFactory.fromAnyRef(UUID.randomUUID().toString))
    .withValue(Node.CONFIG_NODE_STARTUP_TIMEOUT_MS, ConfigValueFactory.fromAnyRef(15000))
    .withValue(GatewayHttp.CONFIG_GATEWAY_HTTP_PORT, ConfigValueFactory.fromAnyRef(0))
    .withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(SystemTestBase.akkaPort.getAndIncrement()))

  def deleteDirectory(path: File) = if (path.exists()) {
    def getRecursively(f: File): Seq[File] = f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
    getRecursively(path).foreach(f => if (!f.delete()) throw new RuntimeException("Failed to delete " + f.getAbsolutePath))
  }

  def jsonStringEntity(s: String) = HttpEntity.Strict(ContentTypes.`application/json`, ByteString("\"" + s + "\""))


  class TestGatewayNode(config: Config, gatewayCreator: => ServicesApi)
    extends Node(config.withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(0))) {

    def this(config: Config) = {
      this(config, Class.forName(config.getString(GatewayHttp.CONFIG_GATEWAY_CLASS)).asSubclass(classOf[ServicesApi]).newInstance())
    }

    import system.dispatcher

    implicit val materializer = ActorMaterializer.create(system)

    lazy val gateway = Await.result(system.actorSelection("/user/controller/gateway").resolveOne(10 seconds), 10 seconds )

    val httpPort: Int = Await.result(startGateway(gatewayCreator), startupTimeout)

    val testSSLContext = {
      val certStore = KeyStore.getInstance(KeyStore.getDefaultType)
      certStore.load(null, null)
      certStore.setCertificateEntry("ca", CertificateFactory.getInstance("X.509")
        .generateCertificate(getClass.getClassLoader.getResourceAsStream("keys/localhost.cer")))
      val certManagerFactory = TrustManagerFactory.getInstance("SunX509")
      certManagerFactory.init(certStore)
      val context = SSLContext.getInstance("TLS")
      context.init(null, certManagerFactory.getTrustManagers, new SecureRandom)
      ConnectionContext.https(context)
    }

    def awaitClusterReady(startUpSequence: => Unit): Unit = {
      val clusterReady = new AtomicBoolean(false)
      system.eventStream.subscribe(system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case GatewayClusterStatus(false) =>
            clusterReady.set(true)
            clusterReady.synchronized(clusterReady.notify)
        }
      })), classOf[GatewayClusterStatus])
      startUpSequence
      clusterReady.synchronized(clusterReady.wait(15000))
      assert(clusterReady.get)
    }

    def uri(path: String) = Uri(s"http://localhost:$httpPort$path")

    def https_uri(path: String) = Uri(s"https://localhost:$httpPort$path")

    def http(method: HttpMethod, uri: Uri): Future[HttpResponse] = {
      http(HttpRequest(method = method, uri = uri))
    }

    def http_get(uri: Uri, headers: List[HttpHeader] = List()): HttpResponse = {
      Await.result(http(HttpRequest(method = HttpMethods.GET, uri = uri, headers = headers)), 30 seconds)
    }

    def http_get(uri: Uri): HttpResponse = {
      Await.result(http(HttpRequest(method = HttpMethods.GET, uri = uri)), 30 seconds)
    }

    val mapper = new ObjectMapper()

    def get_json(response: HttpResponse): JsonNode = {
      val json = Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
      mapper.readValue(json, classOf[JsonNode])
    }

    def get_text(response: HttpResponse): String = {
      Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
    }

    def http_post(uri: Uri, entity: Array[Byte] = Array(), headers: List[HttpHeader] = List()): HttpResponse = {
      Await.result(http(HttpRequest(entity = HttpEntity(entity), method = HttpMethods.POST, uri = uri, headers = headers)), 30 seconds)
    }

    def http_post_json(uri: Uri, json: JsonNode, headers: List[HttpHeader] = List()): HttpResponse = {
      Await.result(http(HttpRequest(entity = HttpEntity(ContentTypes.`application/json`, Encoder.json(json)), method = HttpMethods.POST, uri = uri, headers = headers)), 30 seconds)
    }

    def http(req: HttpRequest) = {
      val decodedResponse: Future[HttpResponse] = Http().singleRequest(req, testSSLContext) flatMap {
        response =>
          response.header[headers.`Content-Encoding`] match {
          case Some(c) if (c.encodings.contains(HttpEncodings.gzip)) =>
            response.entity.dataBytes.map(_.asByteBuffer).runWith(Sink.seq).map {
              byteBufferSequence =>
                val unzipped = fromInputStream(() => new GZIPInputStream(new ByteBufferInputStream(byteBufferSequence.asJava)))
                val unzippedEntity = HttpEntity(response.entity.contentType, unzipped)
                response.copy(entity = unzippedEntity)
            }
          case _ => Future.successful(response)
        }
      }
      decodedResponse.flatMap(_.toStrict(2 seconds))
    }

  }

}


