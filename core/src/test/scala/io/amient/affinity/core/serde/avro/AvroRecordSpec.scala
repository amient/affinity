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

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

//TODO #9 test avro record with PropSpec generating random records
class AvroRecordSpec extends FlatSpec with Matchers {

  import Side._

  "write-read" should "produce the same record" in {
    val b1 = Base()
    assert(b1 == Base(ID(0), LEFT, Seq()))  //defaults
    val b2 = Base(ID(2), LEFT, Seq(ID(1), ID(3)))
    AvroRecord.write(b2, b2.getSchema) // value class set
    val b3 = Base(ID(3), RIGHT, Seq(ID(1), ID(2)))
    val c = Composite(
      Seq(b1, b2, b3),
      Map("b1" -> b1, "b2" -> b2, "b3" -> b3),
      Set(1L, 2L, 3L)
    )
    val bytes = AvroRecord.write(c, c.schema)
    val cc = AvroRecord.read(bytes, classOf[Composite], c.schema)
    c should equal(cc)

  }

  "java.lang.UUID" should "work when represented as Avro ByteBuffer BYTES" in {
    val uuid = UUID.randomUUID()
    val x = AvroUUID(uuid)
    val bytes = AvroRecord.write(x, x.schema)
    val copy = AvroRecord.read(bytes, classOf[AvroUUID], x.schema)
    copy.uuid should be (uuid)
  }

}
