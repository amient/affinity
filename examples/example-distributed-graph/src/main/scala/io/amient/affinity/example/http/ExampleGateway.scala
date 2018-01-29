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

package io.amient.affinity.example.rest

import java.util.NoSuchElementException

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{ActorState, GatewayHttp}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.storage.State
import io.amient.affinity.example.http.handler._
import io.amient.affinity.example.rest.handler._
import io.amient.affinity.example.graph.message.ConfigEntry

import scala.util.control.NonFatal

object ExampleGatewayRoot {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Gateway Node requires 1 argument: <http-port>")
    val httpPort = args(0).toInt
    val config = ConfigFactory.load("example")
      .withValue(new GatewayHttp.Conf().Http.Port().toString, ConfigValueFactory.fromAnyRef(httpPort))

    new Node(config) {
      startGateway(new ExampleGateway)
    }
  }
}
class ExampleGateway extends ExampleGatewayRoot
  with Graph
  with Admin
  with PublicApi
  with Ping
  with Fail

class ExampleGatewayRoot extends GatewayHttp {

  val settings: State[String, ConfigEntry] = global[String, ConfigEntry]("settings")

  override def handleException(headers: List[HttpHeader] = List()): PartialFunction[Throwable, HttpResponse] = {
    case e: IllegalAccessException => Encoder.json(NotFound, "Unauthorized" -> e.getMessage)
    case e: NoSuchElementException => Encoder.json(NotFound, "Haven't got that" -> e.getMessage)
    case e: IllegalArgumentException => Encoder.json(BadRequest, "BadRequest" -> e.getMessage)
    case e: UnsupportedOperationException => Encoder.json(NotImplemented, "Probably maintenance- Please try again..")
    case NonFatal(e) =>
      e.printStackTrace()
      Encoder.json(InternalServerError, "Well, something went wrong but we should be back..")
    case e =>
      e.printStackTrace()
      Encoder.json(ServiceUnavailable, "Something is seriously wrong with our servers..")
  }

}