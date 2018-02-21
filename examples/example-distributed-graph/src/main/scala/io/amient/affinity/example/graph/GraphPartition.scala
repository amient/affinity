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

package io.amient.affinity.example.graph

import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.core.storage.State
import io.amient.affinity.example.graph.message._

import scala.collection.immutable.Set

class GraphPartition extends Partition {

  val graph: State[Int, VertexProps] = state("graph")

  val components: State[Int, Component] = state("components")

  import context.dispatcher

  override def handle: Receive = super.handle orElse {

    /**
      * getting Component object by the Component ID
      */
    case request@GetComponent(cid) => sender.reply(request) {
      components(cid)
    }

    /**
      * Updated component of the vertex.
      * Responds with the Component data previously associated with the vertex
      */
    case command@UpdateComponent(cid, updated) => sender.replyWith(command) {
      components.update(cid, updated)
    }

    case command@DeleteComponent(cid) => sender.replyWith(command) {
      components.remove(cid, command)
    }

    /**
      * getting VertexProps object by Vertex ID
      */
    case request@GetVertexProps(vid) => sender.reply(request) {
      graph(vid)
    }

    case command@UpdateVertexComponent(vid, cid) => sender.replyWith(command) {
      graph.updatedAndMap(vid) {
        case None => throw new NoSuchElementException
        case Some(props) if (props.component == cid) => (None, Some(props), cid)
        case Some(props) => (Some(command), Some(props.withComponent(cid)), props.component)
      }
    }

    /**
      * Add an edge to the graph vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      * Responds Status.Failure if the operation fails, or with Success(VertexProps) holding the updated state
      */
    case command@ModifyGraph(vertex, edge, GOP.ADD) => sender.replyWith(command) {
      graph.updatedAndMap(vertex) {
        case Some(existing) if (existing.edges.exists(_.target == edge.target)) =>
          (None, Some(existing), existing)
        case None =>
          val inserted = VertexProps(System.currentTimeMillis, vertex, Set(edge))
          (Some(inserted), Some(inserted), inserted)
        case Some(existing) => val updated = existing.withEdges(existing.edges + edge)
          (Some(command), Some(updated), updated)
      }
    }


    /**
      * Remove an edge from the graph vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      * Responds Status.Failure if the operation fails, true if the data was modified, false otherwise
      */
    case command@ModifyGraph(vertex, edge, GOP.REMOVE) => sender.replyWith(command) {
      graph.updatedAndMap(vertex) {
        case None => throw new NoSuchElementException
        case Some(existing) if !existing.edges.exists(_.target == edge.target) =>
          throw new IllegalArgumentException("not connected")
        case Some(existing) =>
          val updated = existing.withEdges(existing.edges.filter(_.target != edge.target))
          (Some(command), Some(updated), updated)
      }
    }
  }
}
