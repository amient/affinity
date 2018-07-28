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

package io.amient.affinity.core.http

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.security.{KeyStore, SecureRandom}

import javax.net.ssl.{KeyManagerFactory, SSLContext}
import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.http.scaladsl.model._
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.core.config.CfgStruct

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps

object HttpInterfaceConf extends HttpInterfaceConf {
  override def apply(config: Config) = new HttpInterfaceConf().apply(config)
}

class HttpInterfaceConf extends CfgStruct[HttpInterfaceConf] {

  val Host = string("host", true).doc("host to which the http interface binds to")
  val Port = integer("port", true).doc("port to which the http interface binds to")
  val Prefix = string("prefix", "").doc("http uri prefix for all endpoints on this interface, e.g. '/my-prefix'")
  val Tls = struct("tls", new TlsConf, false)
}

class TlsConf extends CfgStruct[TlsConf] {

  val KeyStoreStandard = string("keystore.standard", "PKCS12").doc("format of the keystore")
  val KeyStorePassword = string("keystore.password", true).doc("password to the keystore file")
  val KeyStoreResource = string("keystore.resource", false).doc("resource which holds the keystore, if file not used")
  val KeyStoreFile = string("keystore.file", false).doc("file which contains the keystore contents, if resource not used")
}

class HttpInterface(httpConf: HttpInterfaceConf)(implicit system: ActorSystem) {

  implicit val materializer = ActorMaterializer.create(system)

  val logger = Logging.getLogger(system, this)

  val systemConf =  Conf(system.settings.config)
  val startupTimeout = systemConf.Affi.Node.StartupTimeoutMs().toLong milliseconds
  val shutdownTimeout =  systemConf.Affi.Node.ShutdownTimeoutMs().toLong milliseconds

  val host: String = httpConf.Host()

  val port: Int = httpConf.Port()

  val prefix: Uri.Path = Uri.Path(httpConf.Prefix())

  val sslContext = if (!httpConf.Tls.isDefined()) None else Some(SSLContext.getInstance("TLS"))
  sslContext.foreach { context =>
    logger.info("Configuring SSL Context")
    val password = httpConf.Tls.KeyStorePassword().toCharArray
    val ks = KeyStore.getInstance(httpConf.Tls.KeyStoreStandard())
    val is = if (httpConf.Tls.KeyStoreResource.isDefined) {
      val keystoreResource = httpConf.Tls.KeyStoreResource()
      logger.info("Configuring SSL KeyStore from resouce: " + keystoreResource)
      getClass.getClassLoader.getResourceAsStream(keystoreResource)
    } else {
      val keystoreFileName = httpConf.Tls.KeyStoreFile()
      logger.info("Configuring SSL KeyStore from file: " + keystoreFileName)
      new FileInputStream(keystoreFileName)
    }
    try {
      ks.load(is, password)
    } finally {
      is.close()
    }
    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)
    context.init(keyManagerFactory.getKeyManagers, null, new SecureRandom)
  }


  val connectionCtx: ConnectionContext = sslContext match {
    case None => Http().defaultServerHttpContext
    case Some(sslContext) => ConnectionContext.https(sslContext)
  }

  val incoming: Source[IncomingConnection, Future[ServerBinding]] = Http().bind(host, port, connectionCtx)

  @volatile private var binding: ServerBinding = null

  def bind(gateway: ActorRef): InetSocketAddress = {
    close()
    logger.debug(s"binding http interface $host:$port$prefix")
    val sink: Sink[IncomingConnection, Future[Done]] = if (prefix.isEmpty) {
      Sink.foreach { connection: IncomingConnection =>
        connection.handleWithAsyncHandler { request: HttpRequest =>
          val responsePromise = Promise[HttpResponse]()
          gateway ! HttpExchange(request, responsePromise)
          responsePromise.future
        }
      }
    } else {
      Sink.foreach { connection: IncomingConnection =>
        connection.handleWithAsyncHandler { request: HttpRequest =>
          if (! request.uri.path.startsWith(prefix)) Future.successful(HttpResponse(status = StatusCodes.NotFound)) else {
            val responsePromise = Promise[HttpResponse]()
            val pathWithoutPrefix = request.uri.path.dropChars(prefix.charCount)
            gateway ! HttpExchange(request.copy(uri = request.uri.copy(path = pathWithoutPrefix)), responsePromise)
            responsePromise.future
          }
        }
      }
    }

    val bindingFuture: Future[Http.ServerBinding] = incoming.to(sink).run()
    binding = Await.result(bindingFuture, startupTimeout)
    logger.info(s"http interface listening on ${binding.localAddress}")
    binding.localAddress
  }

  def close(): Unit = {
    if (binding != null) {
      logger.debug("unbinding http interface")
      Await.result(binding.unbind(), shutdownTimeout)
    }
  }

}

