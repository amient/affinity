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
package io.amient.affinity.avro

import java.util.UUID

import io.amient.affinity.avro.record.{Alias, AvroRecord}
import io.amient.affinity.core.util.ByteUtils

case class AliasedAvro(@Alias("old_name1", "old_name2") name: String) extends AvroRecord

object SimpleEnum extends Enumeration {
  type SimpleEnum = Value
  val A, B, C = Value
}

case class SimpleKey(val id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class SimpleRecord(val id: SimpleKey = SimpleKey(0), val side: SimpleEnum.Value = SimpleEnum.A, val seq: Seq[SimpleKey] = Seq()) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class Record_V1(val items: Seq[SimpleRecord] = Seq(), val removed: Int = 0) extends AvroRecord

case class Record_Current(
                val items: Seq[SimpleRecord] = Seq(),
                val index: Map[String, SimpleRecord] = Map(),
                val setOfPrimitives: Set[Long] = Set() ) extends AvroRecord

case class Record_V3(val items: Seq[SimpleRecord] = Seq(), val index: Map[String, SimpleRecord] = Map()) extends AvroRecord

object AvroUUID {
  def apply(uuid: UUID): AvroUUID = apply(ByteUtils.uuid(uuid))
}

case class AvroUUID(val data: Array[Byte]) extends AvroRecord {
  def uuid: UUID = ByteUtils.uuid(data)
  override def hashCode(): Int = ByteUtils.murmur2(data)
}

case class AvroBytes(raw: Array[Byte], optional: Option[Array[Byte]], listed: List[Array[Byte]]) extends AvroRecord

case class AvroEnums(raw: SimpleEnum.Value = SimpleEnum.A,
                     on: Option[SimpleEnum.Value] = None,
                     sd: Option[SimpleEnum.Value] = Some(SimpleEnum.A),
                     l: List[SimpleEnum.Value] = List(),
                     lo: List[Option[SimpleEnum.Value]] = List(Some(SimpleEnum.B))
                    ) extends AvroRecord

case class AvroNamedRecords(
                             e: SimpleKey = SimpleKey(0),
                             rn: Option[SimpleKey] = None,
                             rs: Option[SimpleKey] = Some(SimpleKey(0)),
                             l: List[SimpleKey] = List(SimpleKey(0)),
                             lo: List[Option[SimpleKey]] = List()) extends AvroRecord

case class AvroPrmitives(
                        bn: Option[Boolean] = None,
                       bs: Option[Boolean] = Some(true),
                       in: Option[Int] = None,
                       is: Option[Int] = Some(Int.MinValue),
                       ln: Option[Long] = None,
                       ls: Option[Long] = Some(Long.MinValue),
                       fn: Option[Float] = None,
                       fs: Option[Float] = Some(Float.MinValue),
                       dn: Option[Double] = None,
                       ds: Option[Double] = Some(Double.MinValue),
                       sn: Option[String] = None,
                       ss: Option[String] = Some("Hello")
                       ) extends AvroRecord