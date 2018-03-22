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

package io.amient.affinity.core.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.UpgradeToWebSocket
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.util.control.NonFatal

object RequestMatchers {

  private val logger = LoggerFactory.getLogger(RequestMatchers.getClass)

  object WEBSOCK {
    def unapply(exchange: HttpExchange): Option[(Path, Query, WebSocketExchange)] = try {
      exchange.request.header[UpgradeToWebSocket] map {
        case upgrade: UpgradeToWebSocket => (exchange.request.uri.path, exchange.request.uri.query(), WebSocketExchange(upgrade, exchange.promise))
      }
    } catch {
      case e: ExceptionWithErrorInfo => logger.warn(e.getMessage); None
      case NonFatal(e) => logger.warn("Error while matching WEBSOCK request", e); None
    }
  }

  object HTTP {
    def unapply(exchange: HttpExchange): Option[(HttpMethod, Path, Query, Promise[HttpResponse])] = try {
      exchange.request.header[UpgradeToWebSocket] match {
        case None => Some((exchange.request.method, exchange.request.uri.path, exchange.request.uri.query(), exchange.promise))
        case _ => None
      }
    } catch {
      case e: ExceptionWithErrorInfo => logger.warn(e.getMessage); None
      case NonFatal(e) => logger.warn("Error while matching HTTP request", e); None
    }
  }

  private def HTTP_(method: HttpMethod, exchange: HttpExchange): Option[(ContentType, RequestEntity, Path, Query, Promise[HttpResponse])] = try {
    exchange.request.method match {
      case `method` =>
        Some((exchange.request.entity.contentType, exchange.request.entity, exchange.request.uri.path, exchange.request.uri.query(), exchange.promise))
      case _ => None
    }
  } catch {
    case e: ExceptionWithErrorInfo => logger.warn(e.getMessage); None
    case NonFatal(e) => logger.warn(s"Error while matching HTTP($method) request", e); None
  }

  object HTTP_POST {
    def unapply(exchange: HttpExchange): Option[(ContentType, RequestEntity, Path, Query, Promise[HttpResponse])] = HTTP_(POST, exchange)
  }

  object HTTP_PUT {
    def unapply(exchange: HttpExchange): Option[(ContentType, RequestEntity, Path, Query, Promise[HttpResponse])] = HTTP_(PUT, exchange)
  }

  object PATH {
    def unapplySeq(path: Path): Option[Seq[String]] = try {
      @tailrec
      def r(p: Path, acc: Seq[String] = Seq()): Seq[String] =
        if (p.isEmpty) acc
        else if (p.startsWithSlash) r(p.tail, acc)
        else if (p.tail.isEmpty) acc :+ p.head.toString
        else r(p.tail, acc :+ p.head.toString)

      Some(r(path))
    } catch {
      case e: ExceptionWithErrorInfo => logger.warn(e.getMessage); None
      case NonFatal(e) => logger.warn(s"Error while matching request PATH($path)", e); None
    }
  }

  object INT {
    def unapply(any: Any): Option[Int] = try {
      Some(Integer.parseInt(any.toString))
    } catch {
      case e: NumberFormatException => logger.warn(e.getMessage); None
      case NonFatal(e) => logger.warn(s"Error while matching INT($any) request path component", e); None
    }
  }

  object LONG {
    def unapply(any: Any): Option[Long] = try {
      Some(java.lang.Long.parseLong(any.toString))
    } catch {
      case e: NumberFormatException => logger.warn(e.getMessage); None
      case NonFatal(e) => logger.warn(s"Error while matching LONG($any) request path component", e); None
    }
  }

  object QUERY {
    def unapplySeq(query: Query): Option[Seq[(String, String)]] = try {
      Some(query.sortBy(_._1))
    } catch {
      case NonFatal(e) => logger.warn(s"Error while matching request QUERY($query)", e); None
    }
  }

}
