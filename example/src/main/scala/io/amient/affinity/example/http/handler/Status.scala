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

package io.amient.affinity.example.rest.handler

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.example.rest.HttpGateway

import scala.collection.Map
import scala.concurrent.duration._

trait Status extends HttpGateway {

  import context.dispatcher

  abstract override def handle: Receive = super.handle orElse {

    case HTTP(GET, PATH("status", INT(p)), _, response) =>
      implicit val timeout = Timeout(1 second)
      val task = cluster ? (p.toInt, "status")
      delegateAndHandleErrors(response, task) {
        case any => Encoder.json(OK, any)
      }

    case HTTP(GET, PATH("status"), _, response) =>
      response.success(Encoder.json(OK, Map(
        "singleton-services" -> describeServices,
        "partition-masters" -> describeRegions
      )))
  }

}
