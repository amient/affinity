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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.{Path, Query}
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.storage.State

import scala.concurrent.Promise
import scala.language.postfixOps

case class ProtectedProfile(hello: String = "world") extends AvroRecord

trait PublicApi extends GatewayHttp {

  private val settings: State[String, ConfigEntry] = global[String, ConfigEntry]("settings")

  abstract override def handle: Receive = super.handle orElse {

    case HTTP(GET, uri@PATH("profile", pii), query, response) => AUTH_DSA(uri, query, response) { (sig: String) =>
      Encoder.json(OK, Map(
        "signature" -> sig,
        "profile" -> ProtectedProfile()
      ))
    }

    case HTTP(GET, uri@PATH("verify"), query, response) => AUTH_DSA(uri, query, response) { (sig: String) =>
      Encoder.json(OK, Some(ProtectedProfile()))
    }

  }

  /**
    * AUTH_DSA can be applied to requests that have been matched in the handler Receive method.
    * It is a partial function with curried closure that is invoked on successful authentication with
    * the server signature that needs to be returned as part of response.
    * This is an encryption-based authentication using the keys and salts stored in the settings state.
    */
  object AUTH_DSA {

    def apply(path: Path, query: Query, response: Promise[HttpResponse])(code: (String) => HttpResponse): Unit = {
      try {
        query.get("signature") match {
          case None => throw new IllegalAccessError
          case Some(sig) =>
            sig.split(":") match {
              case Array(k, clientSignature) =>
                settings(k) match {
                  case None => throw new IllegalAccessError(s"Invalid api key $k")
                  case Some(configEntry) =>
                    if (configEntry.crypto.verify(clientSignature, path.toString)) {
                      val serverSignature = configEntry.crypto.sign(clientSignature)
                      response.success(code(serverSignature))
                    } else {
                      throw new IllegalAccessError(configEntry.crypto.sign(path.toString))
                    }
                }

              case _ => throw new IllegalAccessError
            }
        }
      } catch {
        case e: IllegalAccessError => response.success(Encoder.json(Unauthorized, "Unauthorized"))
        case e: Throwable => response.success(handleException(e))
      }
    }
  }

}
