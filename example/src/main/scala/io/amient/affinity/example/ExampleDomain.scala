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
import io.amient.affinity.core.serde.avro.{AvroRecord, EmbeddedAvroSerde}
import io.amient.affinity.core.util.TimeCryptoProofSHA256

class MyAvroSerde extends EmbeddedAvroSerde  {

  register(classOf[Vertex]) //0
  register(classOf[Edge]) //1
  register(classOf[Component]) //2
  register(classOf[ConfigEntry]) //3
  //TODO try declarative schema evolution by adding enum to the Component

}

final case class Vertex(id: Int) extends AvroRecord[Vertex] {
  override def hashCode(): Int = id.hashCode()
}

final case class Edge(source: Vertex, target: Vertex) extends AvroRecord[Edge] {
  override def hashCode(): Int = source.hashCode()
}

final case class Component(val key: Vertex, val edges: Set[Vertex]) extends AvroRecord[Component] {
  override def hashCode(): Int = key.hashCode
}

final case class ConfigEntry(description: String, @JsonIgnore salt: String) extends AvroRecord[ConfigEntry] {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)
  override def hashCode(): Int = description.hashCode()
}


