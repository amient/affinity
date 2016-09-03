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
package io.amient.affinity.core

import java.io.StringWriter

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

trait HttpRequestMapper {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def apply(request: HttpRequest, response: Promise[HttpResponse], cluster: ActorRef)
           (implicit ctx: ExecutionContext): Unit

  def textValue(status: StatusCode, message: String): HttpResponse = {
    HttpResponse(status, entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, message))
  }

  def htmlValue(status: StatusCode, message: String): HttpResponse = {
    val formattedMessage = message.replace("\n", "<br/>")
    HttpResponse(status,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h3>$formattedMessage</h3s>"))
  }

  def jsonValue(status: StatusCode, value: Any): HttpResponse = {
    val out = new StringWriter
    mapper.writeValue(out, value)
    val json = out.toString()
    HttpResponse(status, entity = HttpEntity(ContentTypes.`application/json`, json))
  }

  def errorValue(errorStatus: StatusCode, ct: ContentType, message: String): HttpResponse = {
    ct match {
      case ContentTypes.`text/html(UTF-8)` => htmlValue(errorStatus, s"<h1> $message</h1>")
      case ContentTypes.`application/json` => jsonValue(errorStatus, Map("error" -> message))
      case _ => textValue(errorStatus, "error: " + message)
    }
  }

  def fulfillAndHandleErrors(promise: Promise[HttpResponse], future: Future[Any], ct: ContentType)
                            (f: Any => HttpResponse)(implicit ctx: ExecutionContext) {
    promise.completeWith(handleErrors(future, ct, f))
  }

  def handleErrors(future: Future[Any], ct: ContentType, f: Any => HttpResponse)
                  (implicit ctx: ExecutionContext): Future[HttpResponse] = {
    future map (f) recover {
      case e: IllegalArgumentException =>
        e.printStackTrace() //log.error("Gateway contains bug! ", e)
        errorValue(InternalServerError, ct, "Eeek! We have a bug..")

      case NonFatal(e) =>
        e.printStackTrace() //log.error("Cluster encountered failure ", e.getMessage)
        errorValue(InternalServerError, ct, "Well, something went wrong but we should be back..")

      case e =>
        errorValue(InternalServerError, ct, "Something is seriously wrong with our servers..")
    }
  }

}
