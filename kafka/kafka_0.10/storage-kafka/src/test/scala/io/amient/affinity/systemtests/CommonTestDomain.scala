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

package io.amient.affinity.systemtests

import java.nio.ByteBuffer

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroRecord
import io.amient.affinity.avro.schema.{CfAvroSchemaRegistry, ZkAvroSchemaRegistry}
import io.amient.affinity.core.util.ByteUtils

object UUID {
  def apply(uuid: java.util.UUID): UUID = apply(ByteBuffer.wrap(ByteUtils.uuid(uuid)))

  def random: UUID = apply(java.util.UUID.randomUUID)
}

case class UUID(val data: ByteBuffer) extends AvroRecord[UUID] {
  def javaUUID: java.util.UUID = ByteUtils.uuid(data.array)
}

case class KEY(id: Int) extends AvroRecord[KEY] {
  override def hashCode(): Int = id.hashCode()
}

case class TestRecord(key: KEY, uuid: UUID, ts: Long = 0L, text: String = "") extends AvroRecord[TestRecord] {
  override def hashCode(): Int = key.hashCode()
}
