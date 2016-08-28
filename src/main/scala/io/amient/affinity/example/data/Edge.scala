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

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

object Edge {
  val schema: Schema = SchemaBuilder.record("Edge_v1")
    .namespace("io.amient.affinity.example").fields()
    .name("source").`type`(Vertex.schema).noDefault()
    .name("target").`type`(Vertex.schema).noDefault()
    .endRecord()
}
final case class Edge(source: Vertex, target: Vertex) extends Record(Edge.schema) {
  put("source", source)
  put("target", target)

  def this(record: GenericRecord) = this(
    new Vertex(record.get("source").asInstanceOf[GenericRecord]),
    new Vertex(record.get("target").asInstanceOf[GenericRecord]))

  def this(initializer: (Schema => GenericRecord)) = this(initializer(Edge.schema))
  override def hashCode(): Int = source.hashCode()

  override def equals(o: scala.Any): Boolean = o match {
    case other: Edge => other.source == source && other.target == target
    case _ => false
  }
}
