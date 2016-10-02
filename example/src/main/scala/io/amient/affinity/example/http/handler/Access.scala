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
import akka.http.scaladsl.model._
import io.amient.affinity.example.ConfigEntry
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.core.util.TimeCryptoProof

import scala.concurrent.Future
import scala.util.control.NonFatal

trait Access extends HttpGateway {

  import context.dispatcher

  abstract override def handle: Receive = super.handle orElse {

    case HTTP(GET, uri@PATH("p", "access", pii), query, response) => AUTH_CRYPTO(uri, query, response) { (sig: String) =>
      Encoder.json(OK, Map(
        "signature" -> sig,
        "pii" -> pii
      ))
    }


    case http@HTTP(GET, PATH("settings"), _, _) => AUTH_ADMIN(http) { (user: String) =>
      try {
        Future.successful {
          throw new RuntimeException("!")
          Encoder.json(OK, Map(
            "credentials" -> user,
            "settings" -> settings.iterator.toMap
          ))
        }
      } catch {
        case NonFatal(e) => Future.failed(e)
      }
    }


    case http@HTTP(POST, PATH("settings", "add"), QUERY(("key", key)), response) => AUTH_ADMIN(http) { (user: String) =>
      settings(key) map {
        case _ => Encoder.json(BadRequest, "That key already exists" -> key)
      } recover {
        case e: NoSuchElementException =>
          val salt = TimeCryptoProof.toHex(TimeCryptoProof.generateSalt())
          settings.put(key, ConfigEntry(key, salt))
          HttpResponse(SeeOther, headers = List(headers.Location(Uri("/settings"))))
      }
    }
  }
}
