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

import io.amient.affinity.core.serde.avro.AvroRecord
import org.scalatest.{FlatSpec, Matchers}

object Side extends Enumeration {
  type Side = Value
  val LEFT, RIGHT = Value
  val symbols = values.toList.map(_.toString)
}

case class Base(val id: Int, val side: Side.Value = Side.LEFT) extends AvroRecord[Base]

case class Composite(val items: Seq[Base] = Seq(), val index: Map[String, Base] = Map()) extends AvroRecord[Composite]

case class _V1_Composite(val items: Seq[Base] = Seq(), val removed: Int = 0) extends AvroRecord[_V1_Composite]


class AvroRecordSpec extends FlatSpec with Matchers {

  import Side._

  "write-read" should "produce the same record" in {
    val b1 = Base(1, LEFT)
    val b2 = Base(2, LEFT)
    val b3 = Base(3, RIGHT)
    val c = Composite(
      Seq(b1, b2, b3),
      Map("b1" -> b1, "b2" -> b2, "b3" -> b3)
    )
    val bytes = AvroRecord.write(c, c.getSchema)
    val cc = AvroRecord.read(bytes, classOf[Composite], c.getSchema)
    assert(c == cc)

  }

}
