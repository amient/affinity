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

import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.core.storage.State
import message.{Component, GOP, VertexProps, _}

import scala.collection.immutable.Set

class GraphPartition extends Partition {

  val graph: State[Int, VertexProps] = state("graph")

  val components: State[Int, Component] = state("components")

  import context.dispatcher

  override def handle: Receive = super.handle orElse {

    /**
      * getting Component object by the Component ID
      */
    case request@GetComponent(cid) => request(sender) ! components(cid)

    /**
      * Updated component of the vertex.
      * Responds with the Component data previously associated with the vertex
      */
    case request@UpdateComponent(cid, updated) => request(sender) ! components.update(cid, updated)

    case request@DeleteComponent(cid) => request(sender) ! components.remove(cid)

    /**
      * getting VertexProps object by Vertex ID
      */
    case request@GetVertexProps(vid) => request(sender) ! graph(vid)

    case request@UpdateVertexComponent(vid, cid) => request(sender) ! {
      graph.getAndUpdate(vid, {
        case None => throw new NoSuchElementException
        case Some(props: VertexProps) if (props.component == cid) => Some(props)
        case Some(props: VertexProps) => Some(props.withComponent(cid))
      }) map {
        _.get.component
      }
    }

    /**
      * Add an edge to the graph vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      * Responds Status.Failure if the operation fails, or with Success(VertexProps) holding the updated state
      */
    case request@ModifyGraph(vertex, edge, GOP.ADD) => request(sender) ! {
      graph.updateAndGet(vertex, {
        case Some(existing) if (existing.edges.exists(_.target == edge.target)) => Some(existing)
        case None => Some(VertexProps(System.currentTimeMillis, vertex, Set(edge)))
        case Some(existing) => Some(existing.withEdges(existing.edges + edge))
      }) map (_.get)
    }


    /**
      * Remove an edge from the graph vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      * Responds Status.Failure if the operation fails, or with Success(VertexProps) holding the updated state
      */
    case request@ModifyGraph(vertex, edge, GOP.REMOVE) => request(sender) ! {
      graph.updateAndGet(vertex ,{
        case None => throw new NoSuchElementException
        case Some(existing) if !existing.edges.exists(_.target == edge.target) => throw new IllegalArgumentException("not connected")
        case Some(existing) => Some(existing.withEdges(existing.edges.filter(_.target != edge.target)))
      }) map (_.get)
    }
  }
}
