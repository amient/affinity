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

import akka.actor.PoisonPill
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.example.rest.ExampleGateway

import scala.concurrent.duration._
import scala.language.postfixOps

trait Fail extends ExampleGateway {

  import context.dispatcher

  abstract override def handle: Receive = super.handle orElse {

    /**
      * kill the entire node
      */
    case HTTP(POST, PATH("kill"), _, response) => context.system.terminate()

    /**
      * terminate gateway
      */
    case HTTP(POST, PATH("stop"), _, response) => self ! PoisonPill

    /**
      * shut down region serving the given partition
      */
    case HTTP(POST, PATH("down", INT(partition)), _, response) =>
      implicit val timeout = Timeout(1 second)
      val task = service("graph") ! (partition, "down")
      Thread.sleep(1000)
      response.success(HttpResponse(MovedPermanently, headers = List(headers.Location(Uri("/")))))

    /**
      * simulate exception in partition
      */
    case HTTP(POST, PATH("fail", INT(partition)), _, response) =>
      implicit val timeout = Timeout(1 second)
      val task = service("graph") ? (partition, new IllegalStateException(System.currentTimeMillis.toString))
      delegateAndHandleErrors(response, task) {
        case any => HttpResponse(status = StatusCodes.Accepted)
      }

    /**
      * simulate bug in partition
      */
    case HTTP(POST, PATH("bug", INT(partition)), _, response) =>
      implicit val timeout = Timeout(1 second)
      val task = service("graph") ? (partition, "message-that-can't-be-handled")
      delegateAndHandleErrors(response, task) {
        case any => Encoder.json(OK, any)
      }

  }

}
