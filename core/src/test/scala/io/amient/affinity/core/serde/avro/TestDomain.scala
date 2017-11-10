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
package io.amient.affinity.core.serde.avro

import java.nio.ByteBuffer
import java.util.UUID

import io.amient.affinity.avro.AvroRecord
import io.amient.affinity.core.util.ByteUtils

object Side extends Enumeration {
  type Side = Value
  val LEFT, RIGHT = Value
}

case class ID(val id: Int) extends AvroRecord[ID] {
  override def hashCode(): Int = id.hashCode()
}

case class Base(val id: ID = ID(0), val side: Side.Value = Side.LEFT, val seq: Seq[ID] = Seq()) extends AvroRecord[Base] {
  override def hashCode(): Int = id.hashCode()
}

case class Composite(
    val items: Seq[Base] = Seq(),
    val index: Map[String, Base] = Map(),
    val setOfPrimitives: Set[Long] = Set() ) extends AvroRecord[Composite]

case class _V1_Composite(val items: Seq[Base] = Seq(), val removed: Int = 0) extends AvroRecord[_V1_Composite]

case class _V3_Composite(val items: Seq[Base] = Seq(), val index: Map[String, Base] = Map()) extends AvroRecord[_V3_Composite]

object AvroUUID {
  def apply(uuid: UUID): AvroUUID = apply(ByteBuffer.wrap(ByteUtils.uuid(uuid)))
}

case class AvroUUID(val data: ByteBuffer) extends AvroRecord[AvroUUID] {
  def uuid: UUID = ByteUtils.uuid(data.array)
  override def hashCode(): Int = data.hashCode()
}