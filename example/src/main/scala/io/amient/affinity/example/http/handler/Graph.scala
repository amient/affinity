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

import java.util.concurrent.ConcurrentHashMap

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes.{OK, SeeOther}
import akka.http.scaladsl.model._
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.transaction.Transaction
import io.amient.affinity.example._
import io.amient.affinity.example.rest.HttpGateway

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal


trait Graph extends HttpGateway {

  import context.dispatcher

  val cache = new ConcurrentHashMap[Int, HttpResponse]()

  abstract override def handle: Receive = super.handle orElse {

    /**
      * GET /vertex/<vertex>
      */
    case HTTP(GET, PATH("vertex", INT(vid)), query, response) =>
      implicit val timeout = Timeout(1 seconds)
      delegateAndHandleErrors(response, ack(cluster, GetVertexProps(vid))) {
        _ match {
          case None => throw new NoSuchElementException
          case Some(vertexProps) => Encoder.json(OK, vertexProps)
        }
      }

    /**
      * GET /component/<component>
      */
    case HTTP(GET, PATH("component", INT(cid)), query, response) =>
      implicit val timeout = Timeout(1 seconds)
      delegateAndHandleErrors(response, ack(cluster, GetComponent(cid))) {
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

  private def connect(v1: Int, v2: Int): Future[Set[Int]] = Transaction(cluster) { transaction =>
    val ts = System.currentTimeMillis
    transaction execute ModifyGraph(v1, Edge(v2, ts), GOP.ADD) flatMap {
      case props1 => transaction execute ModifyGraph(v2, Edge(v1, ts), GOP.ADD) flatMap {
        case props2 => transaction execute collectComponent(v2) flatMap {
          case mergedComponent =>
            val newComponentID = mergedComponent.connected.min
            transaction execute UpdateComponent(newComponentID, mergedComponent)
            if (props1.component != newComponentID) transaction execute DeleteComponent(props1.component)
            if (props2.component != newComponentID) transaction execute DeleteComponent(props2.component)
            Future.sequence(mergedComponent.connected.map { v =>
              transaction execute UpdateVertexComponent(v, newComponentID)
            })
        }
      }
    }
  }

  private def disconnect(v1: Int, v2: Int) = Transaction(cluster) { transaction =>
    val ts = System.currentTimeMillis
    transaction execute ModifyGraph(v1, Edge(v2, ts), GOP.REMOVE) flatMap {
      case props1 => transaction execute ModifyGraph(v2, Edge(v1, ts), GOP.REMOVE) flatMap {
        case props2 => transaction execute collectComponent(v1) flatMap {
          case component1 => transaction execute collectComponent(v2) flatMap {
            case component2 =>
              val newComponentIDS = List(component1.connected.min, component2.connected.min)
              transaction execute UpdateComponent(newComponentIDS(0), component1)
              transaction execute UpdateComponent(newComponentIDS(1), component2)
              if (!newComponentIDS.contains(props1.component)) transaction execute DeleteComponent(props1.component)
              if (!newComponentIDS.contains(props2.component)) transaction execute DeleteComponent(props2.component)
              Future.sequence {
                component1.connected.map { v =>
                  transaction execute UpdateVertexComponent(v, newComponentIDS(0))
                } ++ component2.connected.map { v =>
                  transaction execute UpdateVertexComponent(v, newComponentIDS(1))
                }
              }
          }
        }
      }
    }
  }

  private def collectComponent(vertex: Int): Future[Component] = {
    val promise = Promise[Component]()
    val ts = System.currentTimeMillis
    implicit val timeout = Timeout(10 seconds)
    def collect(queue: Set[Int], agg: Set[Int]): Unit = {
      if (queue.isEmpty) {
        promise.success(Component(ts, agg))
      }
      else ack(cluster, GetVertexProps(queue.head)) map {
        _ match {
          case None => throw new NoSuchElementException
          case Some(VertexProps(_, cid, Edges(connected))) => collect(queue.tail ++ (connected -- agg), agg ++ connected)
        }
      } recover {
        case NonFatal(e) => promise.failure(e)
      }
    }
    collect(Set(vertex), Set(vertex))
    promise.future
  }

}
