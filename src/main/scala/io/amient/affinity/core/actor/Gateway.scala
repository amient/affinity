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

package io.amient.affinity.core.actor

import java.util.Properties

import akka.actor.{Actor, Status, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.core.HttpRequestMapper
import io.amient.affinity.core.cluster.Cluster

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

object Gateway {

  final case class HttpExchange(request: HttpRequest, promise: Promise[HttpResponse])

}

class Gateway(appConfig: Properties, handlerClass: Class[_ <: HttpRequestMapper]) extends Actor {

  val log = Logging.getLogger(context.system, this)

  import context.dispatcher

  val cluster = context.actorOf(new Cluster(appConfig).props(), name = "cluster")

  context.watch(cluster)

  val handler = handlerClass.newInstance()

  override def preStart(): Unit = {
    Await.ready(context.actorSelection(cluster.path).resolveOne()(10 seconds), 10 seconds)
    context.parent ! Controller.GatewayCreated()
  }

  def receive = {

    //Handled http exchanges
    case exchange: Gateway.HttpExchange => handler(exchange.request, exchange.promise, cluster)

    //Management queries
    case m: AddRoutee =>
      log.info("Adding partition: " + m.routee)
      cluster ! m

    case m: RemoveRoutee =>
      log.info("Removing partition: " + m.routee)
      cluster ! m

    case m: GetRoutees =>
      val origin = sender()
      //TODO this timeout should be configurable as it dependes on the size of the cluster and coordinator implementation
      implicit val timeout = Timeout(60 seconds)
      cluster ? m onSuccess { case routees => origin ! routees }

    case Terminated(cluster) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway")

    case _ => sender ! Status.Failure(new IllegalArgumentException)

  }

}
