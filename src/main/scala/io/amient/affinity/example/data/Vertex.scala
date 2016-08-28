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

import io.amient.affinity.core.data.AvroRecord
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}

object Vertex {
  val schema = SchemaBuilder.record("Vertex_v1")
    .namespace("io.amient.affinity.example").fields()
    .name("id").`type`().intType().noDefault()
    .name("cat").`type`().stringType().noDefault()
    .endRecord()
}
final case class Vertex(id: Int, cat: String) extends Record(Vertex.schema) {
  put("id", id)
  put("cat", cat)

  def this(record: GenericRecord) = this(record.get("id").asInstanceOf[Int], record.get("cat").toString)
  def this(initializer: (Schema => GenericRecord)) = this(initializer(Vertex.schema))

  override def hashCode(): Int = id.hashCode()

  override def equals(o: scala.Any): Boolean = o match {
    case other: Vertex => other.id == id && other.cat == cat
    case _ => false
  }
}

final case class VertexV1(id: Int, cat: String) extends AvroRecord(Vertex.schema) {
  override def hashCode(): Int = id.hashCode()
}