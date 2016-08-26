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

package io.amient.affinity.example.graphapi.actor

import akka.actor.{Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.actor.Partition

import scala.concurrent.duration._
import scala.util.{Failure, Success}

//TODO maybe instead of mixing Actor with Handler try annotation mechanism

object LocalHandler {

  final case class Ping(ts: Long = System.currentTimeMillis())

  final case class Describe(partition: Int) {
    override def hashCode = partition
  }

  final case class KillNode(partition: Int) {
    override def hashCode = partition
  }

  final case class CollectUserInput(greeting: String)

  sealed class Graph(val key: Int, val name: String = "") {
    override def hashCode = key.hashCode

    override def equals(obj: scala.Any): Boolean =
      obj.isInstanceOf[Graph] &&
      obj.asInstanceOf[Graph].key == key &&
        obj.asInstanceOf[Graph].name == name

    def toVertex(withEdges: Set[Graph] = Set()) = Vertex(key, name, withEdges)
  }

  final case class Vertex(override val key: Int, override val name: String, val edges: Set[Graph] = Set())
      extends Graph(key, name) {
    def copy(withEdges: Set[Graph]) = Vertex(key, name, withEdges)
  }

  final case class Connect(source: Graph, target: Graph) extends Graph(source.key)

}

class LocalHandler extends Partition {

  final val DEFAULT_KEYSPACE = "graph"

  import LocalHandler._

  import context._

  val userInputMediator = actorOf(Props(new UserInputMediator))

  val cluster = context.actorSelection("/user/controller/gateway/cluster")

  val graph = storage[Int, Vertex]("graph")

  override def receive = {

    case stateError: IllegalStateException => throw stateError

    case Ping(ts) => sender ! s"${parent.path.name}:Pong"

    case Describe(p) => sender ! s"$p:\n" + graph.values.mkString("\n\t")

    case Connect(source, target) => {
      (graph.get(source.key) match {
        case Some(vertex) if (vertex.edges.contains(target)) => None
        case Some(vertex) => Some(vertex.copy(vertex.edges + target))
        case None => Some(source.toVertex(Set(target)))
      }) foreach { updatedVertex =>
        cluster ! Connect(target, source)
        updatedVertex.edges.filter(_ != target).foreach { other =>
          cluster ! Connect(other, target)
        }
        graph.update(source.key, updatedVertex)
      }
    }

    case KillNode(_) =>
      log.warning("killing the entire node " + system.name)
      implicit val timeout = Timeout(10 seconds)
      actorSelection("/user").resolveOne().onSuccess {
        case controller => stop(controller)
      }

    case CollectUserInput(greeting) =>
      implicit val timeout = Timeout(60 seconds)
      val origin = sender()
      val prompt = s"${parent.path.name}: $greeting >"
      userInputMediator ? prompt andThen {
        case Success(userInput: String) =>
          if (userInput == "error") origin ! Status.Failure(throw new RuntimeException(userInput))
          else origin ! userInput
        case Failure(e) => origin ! Status.Failure(e)
      }


    case unknown => sender ! Status.Failure(new IllegalArgumentException(unknown.getClass.getName))

  }
}
