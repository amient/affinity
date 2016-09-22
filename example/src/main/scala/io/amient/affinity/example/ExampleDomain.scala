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

import com.fasterxml.jackson.annotation.JsonIgnore
import io.amient.affinity.core.serde.avro.schema.EmbeddedAvroSchemaProvider
import io.amient.affinity.core.serde.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.util.TimeCryptoProofSHA256

class MyAvroSerde extends AvroSerde with EmbeddedAvroSchemaProvider {
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

}

final case class ConfigEntry(description: String, @JsonIgnore salt: String) extends AvroRecord[ConfigEntry] {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)
  override def hashCode(): Int = description.hashCode()
}

final case class Edge(target: Int, timestamp: Long = 0L) extends AvroRecord[Edge]

final case class VertexProps(edges: Set[Edge] = Set(), component: Set[Int] = Set()) extends AvroRecord[VertexProps]

object GOP extends Enumeration {
  type Side = Value
  val ADD, REMOVE = Value
}

final case class ModifyGraph(val key: Int, val edge: Edge, val op: GOP.Value = GOP.ADD) extends AvroRecord[ModifyGraph]{
  override def hashCode(): Int = key.hashCode
}

final case class Component(val key: Int, val component: Set[Int] = Set()) extends AvroRecord[Component] {
  override def hashCode(): Int = key.hashCode
}


