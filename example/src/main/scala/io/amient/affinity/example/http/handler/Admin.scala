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

package io.amient.affinity.example.http.handler

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, HttpChallenge}
import akka.http.scaladsl.model.{HttpResponse, headers}
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.http.RequestMatchers.{HTTP, INT, PATH, QUERY}
import io.amient.affinity.core.http.{Encoder, HttpExchange}
import io.amient.affinity.core.util.TimeCryptoProof
import io.amient.affinity.example.ConfigEntry
import io.amient.affinity.example.rest.HttpGateway

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait Admin extends HttpGateway {

  import context.dispatcher

  abstract override def handle: Receive = super.handle orElse {

    case http@HTTP(GET, PATH("settings"), _, _) => AUTH_ADMIN(http) { (user: String) =>
      try {
        Future.successful {
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
      Future.successful {
        settings(key) match {
          case Some(existinKey) => Encoder.json(BadRequest, "That key already exists" -> key)
          case None =>
            val salt = TimeCryptoProof.toHex(TimeCryptoProof.generateSalt())
            settings.set(key, ConfigEntry(key, salt))
            Encoder.json(OK, salt)
        }
      }
    }

    case http@HTTP(GET, PATH("status", INT(p)), _, response) => AUTH_ADMIN(http) { (user: String) =>
      implicit val timeout = Timeout(1 second)
      cluster ? (p.toInt, "status") map {
        case any => Encoder.json(OK, any)
      }
    }

    case http@HTTP(GET, PATH("status"), _, response) => AUTH_ADMIN(http)(user => Future.successful {
      Encoder.json(OK, Map(
        "singleton-services" -> describeServices,
        "partition-masters" -> describeRegions
      ))
    })

  }

  /**
    * AUTH_ADMIN is a pattern match extractor that can be used in handlers that want to
    * use Basic HTTP Authentication for administrative tasks like creating new keys etc.
    */
  object AUTH_ADMIN {

    def apply(exchange: HttpExchange)(code: (String) => Future[HttpResponse]): Unit = {

      def executeCode(user: String): Future[HttpResponse] = try {
        code(user)
      } catch {
        case NonFatal(e) => Future.failed(e)
      }

      val auth = exchange.request.header[Authorization]
      val response = exchange.promise
      val credentials = for (Authorization(c@BasicHttpCredentials(username, password)) <- auth) yield c
      settings("admin") match {
        case None =>
          credentials match {
            case Some(BasicHttpCredentials(username, newAdminPassword)) if username == "admin" =>
              settings.set("admin", ConfigEntry("Administrator Account", TimeCryptoProof.toHex(newAdminPassword.getBytes)))
              fulfillAndHandleErrors(response) {
                executeCode(username)
              }
            case _ => response.success(HttpResponse(
              Unauthorized, headers = List(headers.`WWW-Authenticate`(HttpChallenge("BASIC", Some("Create admin password"))))))
          }
        case Some(ConfigEntry(any, adminPassword)) => credentials match {
          case Some(BasicHttpCredentials(username, password)) if username == "admin"
            && TimeCryptoProof.toHex(password.getBytes) == adminPassword =>

            fulfillAndHandleErrors(response) {
              executeCode(username)
            }
          case _ =>
            response.success(HttpResponse(Unauthorized, headers = List(headers.`WWW-Authenticate`(HttpChallenge("BASIC", None)))))
        }
      }

    }

  }

}
