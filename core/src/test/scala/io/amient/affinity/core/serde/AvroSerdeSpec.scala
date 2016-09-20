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

package io.amient.affinity.core.serde

import io.amient.affinity.core.serde.avro.AvroSerde
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class AvroSerdeSpec extends FlatSpec with Matchers {

  /**
    * Data version 1 is written at some point in the past
    */
  val oldSerde = new AvroSerde {
    override def register: Seq[(Schema, Class[_])] = Seq(
      Base.schema -> classOf[Base],
      Composite.schemaV1 -> classOf[_V1_Composite],
      Composite.schemaV2 -> classOf[_V1_Composite]
      //Future schema V2 available in old version - slightly unrealistic for embedded registry but here we're testing
      //the API for handling serialization, not the embedded implementation of registry, i.e. this would be true
      //in case of shared registry, like zookeeper of kafka-based, or confluent schema registry
    )
  }
  val oldValue = oldSerde.toBinary(_V1_Composite(Seq(Base(1, Side.LEFT)), 10))

  /**
    * New version 2 of the case class and schema is added at the end of registry
    * the previous V1 schema version now points to the newest case class.
    */
  val newSerde = new AvroSerde {
    override def register: Seq[(Schema, Class[_])] = Seq(
      Base.schema -> classOf[Base],
      Composite.schemaV1 -> classOf[Composite],
      Composite.schemaV2 -> classOf[Composite]
    )
  }

  /**
    * Backward compatible schemas - data written with Version 1 schema is encountered with
    * newer serde and is rendered via avro schema evolution into the latest case class form.
    */
  val upgradedValue = newSerde.fromBinary(oldValue)
  upgradedValue should be(Composite(Seq(Base(1, Side.LEFT)), Map()))

  val newValue = newSerde.toBinary(upgradedValue)

  /**
    * Forward compatible schemas - newer version of data is encountered by an older serde.
    */
  // FIXME the last obstacle for having the neat API between scala case classes and avro records is getting the compile time schema associated with the class
  //  val downgradedValue = oldSerde.fromBinary(newValue)
  //  downgradedValue should be(_V1_Composite(Seq(Base(1, Side.LEFT)), 0))


}
