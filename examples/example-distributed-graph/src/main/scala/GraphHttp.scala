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

import java.util.concurrent.ConcurrentHashMap

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes.{OK, SeeOther}
import akka.http.scaladsl.model._
import io.amient.affinity.core.actor.{GatewayHttp, WebSocketSupport}
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers._

import scala.language.postfixOps


trait GraphHttp extends GatewayHttp with WebSocketSupport {

  self: GraphApi =>

  import context.dispatcher

  val cache = new ConcurrentHashMap[Int, HttpResponse]()

  val html = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/wsclient.html")).mkString

  abstract override def handle: Receive = super.handle orElse {

    /**
      * WebSocket GET /vertex?id=<vid>
      */
    case HTTP(GET, PATH("vertex"), _, response) => response.success(Encoder.html(OK, html))
    case WEBSOCK(PATH("vertex"), QUERY(("id", INT(vertex))), socket) =>
      connectKeyValueMediator(graphService, "graph", vertex) map {
        keyValueMediator => avroWebSocket(socket, keyValueMediator)
      }

    /**
      * GET /vertex/<vertex>
      */
    case HTTP(GET, PATH("vertex", INT(vid)), query, response) =>
      delegateAndHandleErrors(response, getVertexProps(vid)) {
        _ match {
          case None => throw new NoSuchElementException
          case Some(vertexProps) => Encoder.json(OK, vertexProps)
        }
      }

    /**
      * WebSocket GET /component?id=<vid>
      */
    case HTTP(GET, PATH("component"), _, response) => response.success(Encoder.html(OK, html))
    case WEBSOCK(PATH("component"), QUERY(("id", INT(cid))), socket) =>
      connectKeyValueMediator(graphService, "components", cid) map {
        case keyValueMediator => avroWebSocket(socket, keyValueMediator)
      }

    /**
      * GET /component/<component>
      */
    case HTTP(GET, PATH("component", INT(cid)), query, response) =>
      delegateAndHandleErrors(response, getGraphComponent(cid)) {
        _ match {
          case None => throw new NoSuchElementException
          case Some(component) => Encoder.json(OK, component)
        }
      }

    /**
      * POST /connect/<vertex1>/<vertex2>
      */
    case HTTP(POST, PATH("connect", INT(id), INT(id2)), query, response) =>
      delegateAndHandleErrors(response, connect(id, id2)) {
        case _ => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/vertex/$id"))))
      }

    /**
      * POST /disconnect/<vertex1>/<vertex2>
      */
    case HTTP(POST, PATH("disconnect", INT(id), INT(id2)), query, response) =>
      delegateAndHandleErrors(response, disconnect(id, id2)) {
        case _ => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/vertex/$id"))))
      }

  }

}
