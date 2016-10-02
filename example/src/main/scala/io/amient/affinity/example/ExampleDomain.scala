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
import io.amient.affinity.core.util.TimeCryptoProofSHA256

class MyAvroSerde(system: ExtendedActorSystem) extends ZkAvroSchemaRegistry(system) {
  //0
  register(classOf[ConfigEntry])
  //1
  register(classOf[Edge])
  //2
  register(classOf[VertexProps])
  //3
  register(classOf[ModifyGraph])
  //4
  register(classOf[Component])
  //5
  register(classOf[UpdateComponent])
  //6
  register(classOf[CollectComponent])
  //7
  register(classOf[GetVertexProps])
}

final case class ConfigEntry(description: String, @JsonIgnore salt: String) extends AvroRecord[ConfigEntry] {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)

  override def hashCode(): Int = description.hashCode()
}

final case class Edge(target: Int, timestamp: Long = 0L) extends AvroRecord[Edge]

final case class VertexProps(ts: Long = 1475178519756L, edges: Set[Edge] = Set(), component: Set[Int] = Set()) extends AvroRecord[VertexProps]

final case class Component(vertex: Int, ts: Long = 0L, component: Set[Int] = Set()) extends AvroRecord[Component] {
  override def hashCode(): Int = vertex.hashCode
}

object GOP extends Enumeration {
  type Side = Value
  val ADD, REMOVE = Value
}

final case class ModifyGraph(vertex: Int, edge: Edge, op: GOP.Value = GOP.ADD) extends AvroRecord[ModifyGraph] with Reply[Boolean] {
  override def hashCode(): Int = vertex.hashCode

  def inverse = op match {
    case GOP.ADD => ModifyGraph(vertex, edge, GOP.REMOVE)
    case GOP.REMOVE => ModifyGraph(vertex, edge, GOP.ADD)
  }
}

final case class GetVertexProps(vertex: Int) extends AvroRecord[GetVertexProps] with Reply[VertexProps] {
  override def hashCode(): Int = vertex.hashCode
}

final case class CollectComponent(vertex: Int) extends AvroRecord[CollectComponent] with Reply[Component] {
  override def hashCode(): Int = vertex.hashCode
}

final case class UpdateComponent(vertex: Int, component: Set[Int]) extends AvroRecord[UpdateComponent] with Reply[Component] {
  override def hashCode(): Int = vertex.hashCode
}



