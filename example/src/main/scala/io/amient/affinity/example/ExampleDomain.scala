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

package io.amient.affinity.example

import akka.actor.ExtendedActorSystem
import com.fasterxml.jackson.annotation.JsonIgnore
import io.amient.affinity.core.ack.Reply
import io.amient.affinity.core.serde.avro.AvroRecord
import io.amient.affinity.core.serde.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.core.transaction.Instruction
import io.amient.affinity.core.util.TimeCryptoProofSHA256

class MyAvroSerde(system: ExtendedActorSystem) extends ZkAvroSchemaRegistry(system) {
  register(classOf[ConfigEntry])
  register(classOf[Edge])
  register(classOf[VertexProps])
  register(classOf[GetVertexProps])
  register(classOf[ModifyGraph])
  register(classOf[UpdateVertexComponent])

  register(classOf[Component])
  register(classOf[GetComponent])
  register(classOf[UpdateComponent])
  register(classOf[DeleteComponent])
}

final case class ConfigEntry(description: String, @JsonIgnore salt: String) extends AvroRecord[ConfigEntry] {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)

  override def hashCode(): Int = description.hashCode()
}

final case class Edge(target: Int, timestamp: Long = 0L) extends AvroRecord[Edge]

object Edges {
  def unapply(edges: Set[Edge]): Option[Set[Int]] = Some(edges.map(_.target))
}

final case class VertexProps(ts: Long = 1475178519756L, component: Int = -1, edges: Set[Edge] = Set()) extends AvroRecord[VertexProps] {
  def withComponent(cid: Int) = VertexProps(System.currentTimeMillis, cid, edges)
  def withEdges(newEdges: Set[Edge]) = VertexProps(System.currentTimeMillis, component, newEdges)
}

final case class Component(ts: Long = 0L, connected: Set[Int] = Set()) extends AvroRecord[Component]


sealed trait AvroInstruction[T, R] extends AvroRecord[T] with Instruction[R] {
  def reverse(result: R): Option[AvroInstruction[_, _]]
}

final case class GetVertexProps(vertex: Int) extends AvroRecord[GetVertexProps] with Reply[Option[VertexProps]] {
  override def hashCode(): Int = vertex.hashCode
}

final case class GetComponent(cid: Int) extends AvroRecord[GetComponent] with Reply[Option[Component]] {
  override def hashCode(): Int = cid.hashCode
}

final case class UpdateVertexComponent(vertex: Int, component: Int) extends AvroInstruction[UpdateVertexComponent, Int] {
  override def hashCode(): Int = vertex.hashCode

  override def reverse(result: Int): Option[UpdateVertexComponent] = {
    if (result == component) None else Some(UpdateVertexComponent(vertex, result))
  }

}

object GOP extends Enumeration {
  type Side = Value
  val ADD, REMOVE = Value
}

final case class ModifyGraph(vertex: Int, edge: Edge, op: GOP.Value = GOP.ADD) extends AvroInstruction[ModifyGraph, VertexProps] {
  override def hashCode(): Int = vertex.hashCode

  def reverse(props: VertexProps) = op match {
    case GOP.ADD => Some(ModifyGraph(vertex, edge, GOP.REMOVE))
    case GOP.REMOVE => Some(ModifyGraph(vertex, edge, GOP.ADD))
  }
}


final case class UpdateComponent(cid: Int, component: Component) extends AvroInstruction[UpdateComponent, Option[Component]] {
  override def hashCode(): Int = cid.hashCode
  override def reverse(c: Option[Component]) = c match {
    case Some(result) if (result == component) => None
    case None => Some(DeleteComponent(cid))
    case Some(result) => Some(UpdateComponent(cid, result))
  }
}

final case class DeleteComponent(cid: Int) extends AvroInstruction[DeleteComponent, Option[Component]] {
  override def hashCode(): Int = cid.hashCode

  override def reverse(c: Option[Component]) = c match {
    case None => None
    case Some(result) => Some(UpdateComponent(cid, result))
  }
}


