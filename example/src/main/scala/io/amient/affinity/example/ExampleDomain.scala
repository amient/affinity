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
import org.apache.avro.Schema

class MyAvroSerde extends AvroSerde with EmbeddedAvroSchemaProvider {
  //0
  register(classOf[Vertex])
  //1
  register(classOf[Edge])
  //2 example of declarative schema evolution - here is an older version of the Component with its literal schema snapshot
  register(classOf[Component], new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Component\",\"namespace\":\"io.amient.affinity.example\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"record\",\"name\":\"Vertex\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}},{\"name\":\"edges\",\"type\":{\"type\":\"array\",\"items\":\"Vertex\"}}]}"))
  //3
  register(classOf[ConfigEntry])
  //4 this is the latest verson of the Component class
  register(classOf[Component])
}

final case class Vertex(id: Int) extends AvroRecord[Vertex] {
  override def hashCode(): Int = id.hashCode()
}

final case class Edge(source: Vertex, target: Vertex) extends AvroRecord[Edge] {
  override def hashCode(): Int = source.hashCode()
}

object Operation extends Enumeration {
  type Side = Value
  val ASSIGN, ADD, REMOVE = Value
}


final case class Component(val key: Vertex, val edges: Set[Vertex], op: Operation.Value = Operation.ASSIGN)
  extends AvroRecord[Component] {
  override def hashCode(): Int = key.hashCode
}

final case class ConfigEntry(description: String, @JsonIgnore salt: String) extends AvroRecord[ConfigEntry] {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)
  override def hashCode(): Int = description.hashCode()
}


