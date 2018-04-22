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

import java.net.InetSocketAddress
import javax.net.ssl.SSLContext

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.http.scaladsl.model._
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps

class HttpInterface(val httpHost: String, httpPort: Int, ssl: Option[SSLContext])(implicit system: ActorSystem) {

  implicit val materializer = ActorMaterializer.create(system)

  val log = Logging.getLogger(system, this)

  val connectionCtx: ConnectionContext = ssl match {
    case None => Http().defaultServerHttpContext
    case Some(sslContext) => ConnectionContext.https(sslContext)
  }

  val incoming: Source[IncomingConnection, Future[ServerBinding]] = Http().bind(httpHost, httpPort, connectionCtx)

  @volatile private var binding: ServerBinding = null

  def bind(gateway: ActorRef): InetSocketAddress = {
    close()
    log.debug(s"binding http interface with $httpHost:$httpPort")
    val bindingFuture: Future[Http.ServerBinding] =
      incoming.to(Sink.foreach { connection =>
        connection.handleWithAsyncHandler { request =>
          val responsePromise = Promise[HttpResponse]()

          gateway ! HttpExchange(request, responsePromise)

          responsePromise.future
        }
      }).run()

    binding = Await.result(bindingFuture, 10 seconds)
    log.info(s"http interface listening on ${binding.localAddress}")
    binding.localAddress
  }

  def close(): Unit = {
    if (binding != null) {
      log.debug("unbinding http interface")
      Await.result(binding.unbind(), 15 seconds)
    }
  }

}

