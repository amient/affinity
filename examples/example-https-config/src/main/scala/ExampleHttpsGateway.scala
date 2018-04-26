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

import akka.http.scaladsl.model.HttpMethods.{GET, PUT}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{Accepted, NotFound, OK}
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{Gateway, GatewayHttp}
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH, QUERY}
import io.amient.affinity.core.util.Reply

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

case class GetData(key: String) extends Reply[Option[String]]

case class PutData(key: String, value: String) extends Reply[Option[String]]

class ExampleApiGateway extends Gateway {

  import context.dispatcher

  implicit val scheduler = context.system.scheduler

  implicit val timeout = Timeout(1 second)

  val simpleService = keyspace("simple-keyspace")

  def getData(key: String): Future[Option[String]] = simpleService ?! GetValue(key)

  def putData(key: String, value: String): Future[Option[String]] = simpleService ?! PutValue(key, value)

  override def handle: Receive = super.handle orElse {
    case request@GetData(key) => request(sender) ! getData(key)
    case request@PutData(key, value) => request(sender) ! putData(key, value)
  }

}

class ExampleHttpsGateway extends ExampleApiGateway with GatewayHttp {

  import context.dispatcher

  override def handle: Receive = {

    case HTTP(GET, PATH("ping"), _, response) => response.success(Encoder.json(OK, "pong"))

    case HTTP(GET, PATH("store", key), _, response) => handleWith(response) {
      getData(key) map {
        case None => HttpResponse(NotFound)
        case Some(value: String) => HttpResponse(OK, entity = value)
      }
    }

    case HTTP(PUT, PATH("store", key), QUERY(("value", value)), response) =>
      handleWith(response) {
        putData(key, value) map {
          case None => HttpResponse(Accepted)
          case Some(prevValue: String) => HttpResponse(OK, entity = prevValue)
        }
      }

  }
}
