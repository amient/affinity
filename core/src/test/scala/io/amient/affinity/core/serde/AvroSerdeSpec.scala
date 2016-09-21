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

import io.amient.affinity.core.serde.avro.schema.EmbeddedAvroSchemaProvider
import io.amient.affinity.core.serde.avro.{AvroRecord, AvroSerde}
import org.scalatest.{FlatSpec, Matchers}

class AvroSerdeSpec extends FlatSpec with Matchers {

  /**
    * Data version 1 is written at some point in the past
    */
  val oldSerde = new AvroSerde with EmbeddedAvroSchemaProvider {
    register(classOf[_V1_Composite])

    //Future schema V2 available in old version - slightly unrealistic for embedded registry but here we're testing
    //the API for handling serialization, not the embedded implementation of registry, i.e. this would be true
    //in case of shared registry, like zookeeper of kafka-based, or confluent schema registry
    register(classOf[_V1_Composite].getName, AvroRecord.inferSchema(classOf[Composite]))
    register(classOf[Base])
  }

  /**
    * New version 2 of the case class and schema is added at the end of registry
    * the previous V1 schema version now points to the newest case class.
    */
  val newSerde = new AvroSerde with EmbeddedAvroSchemaProvider {
    register(classOf[Composite], AvroRecord.inferSchema(classOf[_V1_Composite]))
    register(classOf[Composite])
    register(classOf[Base])
  }

  "Schema Registry" should "preserve strict ordering when registering classes" in {
    oldSerde.schema(classOf[_V1_Composite]) should be(Some(1))
    oldSerde.schema(classOf[Base]) should be(Some(2))
    newSerde.schema(classOf[Composite]) should be(Some(1))
    newSerde.schema(classOf[Base]) should be(Some(2))
  }

  "Data written with an older serde" should "be rendered into the current representation in a backward-compatible way" in {
    val oldValue = oldSerde.toBinary(_V1_Composite(Seq(Base(1, Side.LEFT)), 10))
    val renderedValue = newSerde.fromBinary(oldValue)
    renderedValue should be(Composite(Seq(Base(1, Side.LEFT)), Map()))
  }

  "Data Written with a newer serde" should "be rendered into the the current representation in a forward-compatible way" in {
    val newValue = newSerde.toBinary(Composite(Seq(Base(1, Side.LEFT)), Map("1" -> Base(1, Side.LEFT))))
    val downgradedValue = oldSerde.fromBinary(newValue)
    downgradedValue should be(_V1_Composite(Seq(Base(1, Side.LEFT)), 0))
  }


}
