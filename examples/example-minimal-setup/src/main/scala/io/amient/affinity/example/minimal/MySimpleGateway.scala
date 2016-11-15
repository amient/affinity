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

package io.amient.affinity.example.minimal

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Gateway
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH, QUERY}

import scala.concurrent.duration._
import scala.language.postfixOps

class MySimpleGateway extends Gateway {

  import context.dispatcher

  implicit val scheduler = context.system.scheduler

  implicit val timeout = Timeout(1 second)

  override def handle: Receive = {

    case HTTP(GET, PATH("ping"), _, response) => response.success(Encoder.json(OK, "pong"))

    case HTTP(GET, PATH("get", key), _, response) =>
      delegateAndHandleErrors(response, cluster ack GetValue(key)) {
        case None => HttpResponse(NotFound)
        case Some(value: String) => HttpResponse(OK, entity = value)
      }

    case HTTP(GET, PATH("put", key), QUERY(("value", value)), response) =>
      delegateAndHandleErrors(response, cluster ack PutValue(key, value)) {
        case None => HttpResponse(Accepted)
        case Some(prevValue: String) => HttpResponse(OK, entity = prevValue)
      }

  }
}
