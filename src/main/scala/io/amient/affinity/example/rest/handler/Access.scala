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
import io.amient.affinity.example.rest.HttpGateway

trait Access extends HttpGateway {

  abstract override def handle: Receive = super.handle orElse {
//
//    case HTTP(GET, PATH("p", "access", service, person), AUTH(query, signature), response) =>
//      response.success(jsonValue(OK, Map(
//        "signature" -> signature,
//        "service" -> service,
//        "person" -> person
//      )))

    case HTTP(GET, PATH("p", "access", service, person), _ , response) =>
      response.success(handleError(Unauthorized))
  }
}
