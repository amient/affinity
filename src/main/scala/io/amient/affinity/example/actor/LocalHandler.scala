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

package io.amient.affinity.example.actor

import java.util.Properties

import akka.actor.{Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.example.data.AvroSerde
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreSimpleMap}
import io.amient.affinity.example.data._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class LocalHandler(config: Properties) extends Partition {

  final val DEFAULT_KEYSPACE = "graph"

  import context._

  val userInputMediator = actorOf(Props(new UserInputMediator))

  val cluster = context.actorSelection("/user/controller/gateway/cluster")

  val graph = new KafkaStorage[Vertex, Component](topic = "graph", partition) with MemStoreSimpleMap[Vertex, Component] {

    val serde = new AvroSerde()

    override def serialize: (Vertex, Component) => (Array[Byte], Array[Byte]) = (k, v) => {
      (serde.toBytes(k), serde.toBytes(v))
    }

    override def deserialize: (Array[Byte], Array[Byte]) => (Vertex, Component) = (k, v) => {
      (serde.fromBytes(k, classOf[Vertex]), serde.fromBytes(v, classOf[Component]))
    }
  }

  //TODO provide function that can detected master-standby status changes
  graph.boot(() => true)

  override def receive = {

    case stateError: IllegalStateException => throw stateError

    case (ts: Long, "ping") => sender ! (self.path.name, "pong")

    case (p: Int, "describe") =>
      sender ! graph.iterator.map(_._2).toList

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

    case (ts: Long, "collect-user-input") =>
      implicit val timeout = Timeout(60 seconds)
      val origin = sender()
      val prompt = s"${parent.path.name} > "
      userInputMediator ? prompt andThen {
        case Success(userInput: String) =>
          if (userInput == "error") origin ! Status.Failure(throw new RuntimeException(userInput))
          else origin ! userInput
        case Failure(e) => origin ! Status.Failure(e)
      }

    case unknown => sender ! Status.Failure(new IllegalArgumentException(unknown.getClass.getName))

  }
}
