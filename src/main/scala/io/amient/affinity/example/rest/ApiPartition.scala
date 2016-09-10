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

package io.amient.affinity.example.rest

import java.util.Properties

import akka.actor.Status
import io.amient.affinity.core.actor.Service
import io.amient.affinity.core.storage.MemStoreSimpleMap
import io.amient.affinity.example.data.{Component, _}

class ApiPartition(config: Properties) extends Service {

  //TODO it is a bit obscure that the partition is passed from Region via path component
  val partition = self.path.name.split("-").last.toInt

  //partitioned memstore
  val graph = new AvroKafkaStorage[Vertex, Component](topic = "graph", partition,
    classOf[Vertex], classOf[Component]) with MemStoreSimpleMap[Vertex, Component]

  //TODO provide function that can detect master-standby status changes
  graph.boot(() => true)

  override def receive = {

    case (p: Int, stateError: IllegalStateException) => throw stateError

    case (ts: Long, "ping") => sender ! (self.path.name, "pong")

    case (p: Int, "describe") =>
      sender ! Map(
        "partition" -> partition,
        "graph" -> graph.iterator.map(_._2).toList)

    case vertex: Vertex => sender ! graph.get(vertex)

    case component@Component(vertex, edges) =>
      graph.get(vertex) match {
        case Some(existing) if (existing.edges.forall(edges.contains)) =>
          sender ! false

        case None =>
          graph.put(vertex, Some(component))
          edges.foreach { edge => cluster ! Component(edge, Set(vertex)) }
          sender ! true

        case Some(existing) =>
          val additionalEdges = edges.diff(existing.edges)
          if (additionalEdges.isEmpty) {
            sender ! false
          } else {
            graph.put(vertex, Some(Component(vertex, existing.edges ++ additionalEdges)))
            existing.edges.foreach { connected =>
              cluster ! Component(connected, additionalEdges)
            }
            additionalEdges.foreach { connected =>
              cluster ! Component(connected, Set(vertex))
            }
            sender ! true
          }
      }
  }
}
