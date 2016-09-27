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

import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{Matchers, PropSpec}

class AvroRecordSpec extends PropSpec with PropertyChecks with Matchers {

  import Side._

  property("Case Class constructor default arguments are AvroRecord defaults") {
    val b = Base()
    assert(b == Base(ID(0), LEFT, Seq()))
    AvroRecord.read(AvroRecord.write(b, b.schema), classOf[Base], b.schema) should equal(Base(ID(0), LEFT, Seq()))
    val c = Composite()
    assert(c == Composite(Seq(), Map(), Set()))
    AvroRecord.read(AvroRecord.write(c, c.schema), classOf[Composite], c.schema) should equal(Composite(Seq(), Map(), Set()))
  }

  def uuids: Gen[UUID] = for {
    hi <- arbitrary[Long]
    lo <- arbitrary[Long]
  } yield new UUID(hi, lo)

  property("java.lang.UUID can be represented as Avro Bytes") {
    forAll(uuids) { uuid: UUID =>
      val x = AvroUUID(uuid)
      val bytes = AvroRecord.write(x, x.schema)
      val copy = AvroRecord.read(bytes, classOf[AvroUUID], x.schema)
      copy.uuid should be(uuid)
    }
  }
  def bases: Gen[Base] = for {
    id <- arbitrary[Int]
    side <- Gen.oneOf(Side.LEFT, Side.RIGHT)
    ints <- listOf(arbitrary[Int])
  } yield Base(ID(id), side, ints.map(ID(_)))

  def composites: Gen[Composite] = for {
     nitems <- Gen.choose(1, 2)
     items <- listOfN(nitems, bases)
     keys <- listOfN(nitems, Gen.alphaStr)
     longs <- listOf(arbitrary[Long])
  } yield Composite(items, keys.zip(items).toMap, longs.toSet)

  property("AvroRecord.write is fully reversible by AvroRecord.read") {
    forAll(composites) { composite: Composite =>
      val bytes = AvroRecord.write(composite, composite.schema)
      AvroRecord.read(bytes, classOf[Composite], composite.schema) should equal(composite )
    }
  }


}
