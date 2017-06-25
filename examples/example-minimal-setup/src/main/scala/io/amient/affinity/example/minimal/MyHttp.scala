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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH, QUERY}

import scala.language.postfixOps

class MyHttp extends GatewayHttp with MyApi {

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
