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
import akka.util.Timeout
import io.amient.affinity.core.actor.Service
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreSimpleMap}
import io.amient.affinity.example.data.{AvroSerde, _}

import scala.concurrent.duration._

class RestApiPartition(config: Properties) extends Service {

  final val DEFAULT_KEYSPACE = "graph"

  import context._

  abstract class AvroKafkaStorage[K <: AnyRef, V <:AnyRef ](topic: String, partition: Int, keyClass: Class[K], valueClass: Class[V])
    extends KafkaStorage[K, V](topic, partition) {
    val serde = new AvroSerde()

    override def serialize: (K, V) => (Array[Byte], Array[Byte]) = (k, v) => {
      (serde.toBytes(k), serde.toBytes(v))
    }

    override def deserialize: (Array[Byte], Array[Byte]) => (K, V) = (k, v) => {
      (serde.fromBytes(k, keyClass), serde.fromBytes(v, valueClass))
    }
  }

  //TODO it is a bit obscure that the partition is passed from Region via path component
  val partition = self.path.name.split("-").last.toInt

  //partitioned memstore
  val graph = new AvroKafkaStorage[Vertex, Component](topic = "graph", partition,
    classOf[Vertex], classOf[Component]) with MemStoreSimpleMap[Vertex, Component]

  //TODO provide function that can detected master-standby status changes
  graph.boot(() => true)

  override def receive = {

    case stateError: IllegalStateException => throw stateError

    case (ts: Long, "ping") => sender ! (self.path.name, "pong")

    case (p: Int, "describe") =>
      sender ! Map (
        "partition" -> partition,
        "graph" -> graph.iterator.map(_._2).toList)

    case Edge(source, target) => {
      (graph.get(source) match {
        case Some(component) if (component.edges.contains(target)) => None
        case Some(component) => Some(Component(component.key, component.edges + target))
        case None => Some(Component(source, Set(target)))
      }) foreach { updatedComponent =>
        graph.put(source, Some(updatedComponent))
        cluster ! Edge(target, source)
        updatedComponent.edges.filter(_ != target).foreach { other =>
          cluster ! Edge(other, target)
        }
      }
    }

    case (p: Int, "kill-node") =>
      log.warning("killing the entire node " + system.name)
      implicit val timeout = Timeout(10 seconds)
      actorSelection("/user").resolveOne().onSuccess {
        case controller => stop(controller)
      }


    case unknown => sender ! Status.Failure(new IllegalArgumentException(unknown.getClass.getName))

  }
}
