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
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.example.data.MyAvroSerde
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreSimpleMap}
import io.amient.affinity.example.data.{Component, _}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class ApiPartition(config: Properties) extends Partition {

  val graph = state {
    new KafkaStorage[Vertex, Component](topic = "graph", partition, classOf[MyAvroSerde], classOf[MyAvroSerde])
      with MemStoreSimpleMap[Vertex, Component]
  }

  import context.dispatcher

  implicit val timeout = Timeout(10 seconds)

  override def receiveService: Receive = {

    /**
      * Simulating Partition Failure - the default supervision should restart this actor
      * after execption is thrown
      */
    case (p: Int, stateError: IllegalStateException) =>
      require(p == partition)
      throw stateError

    /**
      * Describe partition and its stats
      */
    case (p: Int, "describe") =>
      require(p == partition)
      sender ! Map(
        "partition" -> partition,
        "graph" -> Map(
          "size" -> graph.size
        ))

    /**
      * Viewing a Component by Vertex key
      */
    case vertex: Vertex => graph.get(vertex) match {
      case None => sender ! Status.Failure(new NoSuchElementException)
      case Some(component) => sender ! component
    }

    /**
      * Connecting a Vertex to 1 or more other Vertices
      * This is done recursively and reliably using Ask instead of Tell.
      * Recoverable failures are attempted to be rolled back by compensation.
      * Responds with true if the operation resulted in graph modification,
      *   false if no changes were made.
      */
    case component@Component(vertex, newEdges) =>
      graph.get(vertex) match {
        case Some(existing) if (existing.edges.forall(newEdges.contains)) => sender ! false
        case None => createAndConnectNew(component)
        case Some(existing) => connectExisting(existing, newEdges)
      }

  }

  private def createAndConnectNew(component: Component): Unit = {
    try {
      val vertex = component.key
      val edges = component.edges
      graph.put(vertex, Some(component))

      val propagate = edges.toList.map {
        cluster ? Component(_, Set(vertex))
      }.map(_ map {
        case _ => true
      } recover {
        case t: Throwable => false
      })

      val origin = sender
      for (success <- Future.sequence(propagate)) {
        if (!success.forall(_ == true)) {
          //compensate failure - TODO use atomic version operation on the graph because this is async and some other call may have modified the component
          graph.put(vertex, None)
          //TODO compensate those calls that succeeded during propagation
          origin ! Status.Failure(new IllegalStateException("propagation failed on new component - attempting to compensate"))
        } else {
          origin ! true
        }
      }
    } catch {
      case NonFatal(e) => sender ! Status.Failure(e)
    }
  }

  private def connectExisting(existing: Component, newEdges: Set[Vertex]): Unit = {
    try {
      val vertex = existing.key
      val additionalEdges = newEdges.diff(existing.edges)
      if (additionalEdges.isEmpty) {
        sender ! false
      } else {
        graph.put(vertex, Some(Component(vertex, existing.edges ++ additionalEdges)))

        val propagate = existing.edges.toList.map {
          cluster ? Component(_, additionalEdges)
        } ++ additionalEdges.toList.map {
          cluster ? Component(_, Set(vertex))
        } map (_ map {
          case _ => true
        } recover {
          case t: Throwable => false
        })

        val origin = sender
        for (success <- Future.sequence(propagate)) {
          if (!success.forall(_ == true)) {
            //compensate failure - TODO use atomic version operation on the graph because this is async and some other call may have modified the component
            graph.put(vertex, Some(existing))
            //TODO compensate those calls that succeeded during propagation
            origin ! Status.Failure(new IllegalStateException("propagation failed on existing component - attempting to compensate"))
          } else {
            origin ! true
          }
        }
      }
    } catch {
      case NonFatal(e) => sender ! Status.Failure(e)
    }
  }


}
