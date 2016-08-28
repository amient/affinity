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

package io.amient.affinity.example.data

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._

object Component {
  val schema = SchemaBuilder.record("Component_v1").namespace("io.amient.affinity.example")
    .fields()
    .name("key").`type`(Vertex.schema).noDefault()
    .name("edges").`type`().array().items().`type`(Vertex.schema).noDefault()
    .endRecord()
}

final case class Component(val key: Vertex, val edges: Set[Vertex]) extends Record(Component.schema) {
  put("key", key)
  put("edges", edges.asJava)

  def this(record: GenericRecord) = this(
    new Vertex(record.get("key").asInstanceOf[GenericRecord]), {
      record.get("edges").asInstanceOf[GenericData.Array[_]].iterator.asScala.map {
        item => new Vertex(item.asInstanceOf[GenericRecord])
      }.toSet
    })

  def this(initializer: (Schema => GenericRecord)) = this(initializer(Component.schema))

  override def hashCode(): Int = key.hashCode()

  override def equals(o: scala.Any): Boolean = o match {
    case other: Component => other.key == key && other.edges == edges
    case _ => false
  }
}
