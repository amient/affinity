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

package io.amient.affinity.model.graph

import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.GatewayApi
import io.amient.affinity.core.transaction.Transaction
import io.amient.affinity.model.graph.message._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.language.postfixOps

trait GraphLogic extends GatewayApi {

  import context.dispatcher

  implicit val scheduler = context.system.scheduler


  protected def getVertexProps(vid: Int): Future[Option[VertexProps]] = {
    implicit val timeout = Timeout(1 seconds)
    service("graph") ack GetVertexProps(vid)
  }

  protected def getGraphComponent(cid: Int): Future[Option[Component]] = {
    implicit val timeout = Timeout(1 seconds)
    service("graph") ack GetComponent(cid)
  }

  protected def connect(v1: Int, v2: Int): Future[Set[Int]] = {
    implicit val timeout = Timeout(5 seconds)
    Transaction(service("graph")) { transaction =>
      val ts = System.currentTimeMillis
      transaction execute ModifyGraph(v1, Edge(v2, ts), GOP.ADD) flatMap {
        case props1 => transaction execute ModifyGraph(v2, Edge(v1, ts), GOP.ADD) flatMap {
          case props2 => transaction apply collectComponent(v2) flatMap {
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
  }

  protected def disconnect(v1: Int, v2: Int): Future[Set[Int]] = {
    implicit val timeout = Timeout(5 seconds)
    Transaction(service("graph")) { transaction =>
      val ts = System.currentTimeMillis
      transaction execute ModifyGraph(v1, Edge(v2, ts), GOP.REMOVE) flatMap {
        case props1 => transaction execute ModifyGraph(v2, Edge(v1, ts), GOP.REMOVE) flatMap {
          case props2 => transaction apply collectComponent(v1) flatMap {
            case component1 => transaction apply collectComponent(v2) flatMap {
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
  }

  private def collectComponent(vertex: Int): Future[Component] = {
    val promise = Promise[Component]()
    val ts = System.currentTimeMillis
    implicit val timeout = Timeout(1 seconds)
    def collect(queue: Set[Int], agg: Set[Int]): Unit = {
      if (queue.isEmpty) {
        promise.success(Component(ts, agg))
      }
      else service("graph") ack GetVertexProps(queue.head) map {
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
