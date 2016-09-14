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

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.amient.affinity.core.actor.Gateway

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

object HttpInterface {

  final val CONFIG_HTTP_HOST = "http.host"
  final val CONFIG_HTTP_PORT = "http.port"

  def fromConfig(appConfig: Properties)(implicit system: ActorSystem): Option[HttpInterface] = {
    if (!appConfig.containsKey(CONFIG_HTTP_PORT)) None else {
      val httpHost = appConfig.getProperty(CONFIG_HTTP_HOST, "localhost")
      val httpPort = appConfig.getProperty(CONFIG_HTTP_PORT, "8080").toInt
      Some(new HttpInterface(httpHost, httpPort))
    }
  }

}

//TODO http://doc.akka.io/docs/akka/2.4.10/scala/http/server-side-https-support.html
class HttpInterface(val httpHost: String, val httpPort: Int)(implicit system: ActorSystem) {

  implicit val materializer = ActorMaterializer.create(system)

  val log = Logging.getLogger(system, this)

  val incoming: Source[IncomingConnection, Future[ServerBinding]] = Http().bind(httpHost, httpPort)

  @volatile private var binding: ServerBinding = null

  def bind(gateway: ActorRef): Unit = {
    close()
    log.info(s"binding http interface to http  $httpHost:$httpPort")
    val bindingFuture: Future[Http.ServerBinding] =
      incoming.to(Sink.foreach { connection =>
        connection.handleWithAsyncHandler { request =>

          val responsePromise = Promise[HttpResponse]()

          gateway ! Gateway.HttpExchange(request, responsePromise)

          responsePromise.future
        }
      }).run()

    binding = Await.result(bindingFuture, 10 seconds)
  }

  log.info(s"Akka Http Server online at http://$httpHost:$httpPort/\nPress ^C to stop...")

  def close(): Unit = {
    if (binding != null) {
      log.info("unbinding http interface")
      Await.result(binding.unbind(), 15 seconds)
    }
  }

}

