/*
 * Copyright 2016-2017 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.example.minimal

import akka.http.scaladsl.model.HttpMethods.{GET, PUT}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{Accepted, NotFound, OK}
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayApi, GatewayHttp}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH, QUERY}
import io.amient.affinity.core.util.Reply

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

trait MyApi {
  def getData(key: String): Future[Option[String]]

  def putData(key: String, value: String): Future[Option[String]]

}

case class GetData(key: String) extends Reply[Option[String]]
case class PutData(key: String, value: String) extends Reply[Option[String]]

trait MyApiNode extends Node with MyApi {

  import system.dispatcher

  implicit val timeout = Timeout(1 second)

  val gateway = Await.result(system.actorSelection("/user/controller/gateway").resolveOne(), timeout.duration)

  def getData(key: String): Future[Option[String]] = gateway ack GetData(key)

  def putData(key: String, value: String): Future[Option[String]] = gateway ack PutData(key, value)
}

class MyApiGateway extends GatewayApi with MyApi {

  import context.dispatcher

  implicit val scheduler = context.system.scheduler

  implicit val timeout = Timeout(1 second)

  def getData(key: String): Future[Option[String]] = service("simple-keyspace") ack GetValue(key)

  def putData(key: String, value: String): Future[Option[String]] = service("simple-keyspace") ack PutValue(key, value)

  override def handle: Receive = super.handle orElse {
    case request@GetData(key) => sender.replyWith(request) { getData(key) }
    case cmd@PutData(key, value) => sender.replyWith(cmd) { putData(key, value) }
  }

}

class MyHttpGateway extends MyApiGateway with GatewayHttp {

  import context.dispatcher

  override def handle: Receive = {

    case HTTP(GET, PATH("ping"), _, response) => response.success(Encoder.json(OK, "pong"))

    case HTTP(GET, PATH("store", key), _, response) =>
      delegateAndHandleErrors(response, getData(key)) {
        case None => HttpResponse(NotFound)
        case Some(value: String) => HttpResponse(OK, entity = value)
      }

    case HTTP(PUT, PATH("store", key), QUERY(("value", value)), response) =>
      delegateAndHandleErrors(response, putData(key, value)) {
        case None => HttpResponse(Accepted)
        case Some(prevValue: String) => HttpResponse(OK, entity = prevValue)
      }

  }
}
