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

import io.amient.affinity.avro.record.AvroRecord
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class AvroRecordPropSpec extends PropSpec with PropertyChecks with Matchers {

  import SimpleEnum._

  property("Case Class constructor default arguments are AvroRecord defaults") {
    val b = SimpleRecord()
    assert(b == SimpleRecord(SimpleKey(0), A, Seq()))
    AvroRecord.read[SimpleRecord](AvroRecord.write(b, b.schema), b.schema) should equal(SimpleRecord(SimpleKey(0), A, Seq()))
    val c = Record_Current()
    assert(c == Record_Current(Seq(), Map(), Set()))
    AvroRecord.read[Record_Current](AvroRecord.write(c, c.schema), c.schema) should equal(Record_Current(Seq(), Map(), Set()))
  }

  def uuids: Gen[UUID] = for {
    hi <- arbitrary[Long]
    lo <- arbitrary[Long]
  } yield new UUID(hi, lo)

  property("java.lang.UUID can be represented as Avro Bytes") {
    forAll(uuids) { uuid: UUID =>
      val x = AvroUUID(uuid)
      val bytes = AvroRecord.write(x, x.schema)
      val copy = AvroRecord.read[AvroUUID](bytes, x.schema)
      copy.uuid should be(uuid)
    }
  }
  def bases: Gen[SimpleRecord] = for {
    id <- arbitrary[Int]
    side <- Gen.oneOf(SimpleEnum.A, SimpleEnum.B)
    ints <- listOf(arbitrary[Int])
  } yield SimpleRecord(SimpleKey(id), side, ints.map(SimpleKey(_)))

  def composites: Gen[Record_Current] = for {
     nitems <- Gen.choose(1, 2)
     items <- listOfN(nitems, bases)
     keys <- listOfN(nitems, Gen.alphaStr)
     longs <- listOf(arbitrary[Long])
  } yield Record_Current(items, keys.zip(items).toMap, longs.toSet)

  property("AvroRecord.write is fully reversible by AvroRecord.read") {
    forAll(composites) { composite: Record_Current =>
      val bytes = AvroRecord.write(composite, composite.schema)
      AvroRecord.read[Record_Current](bytes, composite.schema) should equal(composite )
    }
  }


}
