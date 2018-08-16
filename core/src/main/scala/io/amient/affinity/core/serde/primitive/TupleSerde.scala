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

package io.amient.affinity.core.serde.primitive

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config
import io.amient.affinity.core.serde.{AbstractWrapSerde, Serde, Serdes}
import io.amient.affinity.core.util.ByteUtils

class TupleSerde(serdes: Serdes) extends AbstractWrapSerde(serdes) with Serde[Product] {

  def this(system: ExtendedActorSystem) = this(Serde.tools(system))
  def this(config: Config) = this(Serde.tools(config))

  override def identifier: Int = 132

  override def toBytes(p: Product): Array[Byte] = {
    var result = new Array[Byte](4)
    ByteUtils.putIntValue(p.productArity, result, 0)
    p.productIterator.foreach {
      m =>
        val bytes = toBinaryWrapped(m.asInstanceOf[AnyRef])
        val tmp = new Array[Byte](result.length + bytes.length + 4)
        Array.copy(result, 0, tmp, 0, result.length)
        ByteUtils.putIntValue(bytes.length, tmp, result.length)
        Array.copy(bytes, 0, tmp, 4 + result.length, bytes.length)
        result = tmp
    }
    result
  }

  override protected def fromBytes(bytes: Array[Byte]): Product = {
    val arity = ByteUtils.asIntValue(bytes, 0)
    var tmp = scala.collection.immutable.List[Any]()
    var offset = 4
    (1 to arity).foreach { a =>
      val len = ByteUtils.asIntValue(bytes, offset)
      offset += 4
      val b = new Array[Byte](len)
      Array.copy(bytes, offset, b, 0, len)
      val element = fromBinaryWrapped(b)
      offset += len
      tmp :+= element
    }
    tmp match {
      case p1 :: Nil => Tuple1(p1)
      case p1 :: p2 :: Nil => Tuple2(p1, p2)
      case p1 :: p2 :: p3 :: Nil => Tuple3(p1, p2, p3)
      case p1 :: p2 :: p3 :: p4 :: Nil => Tuple4(p1, p2, p3, p4)
      case p1 :: p2 :: p3 :: p4 :: p5 :: Nil => Tuple5(p1, p2, p3, p4, p5)
      case p1 :: p2 :: p3 :: p4 :: p5 :: p6 :: Nil => Tuple6(p1, p2, p3, p4, p5, p6)
      case p1 :: p2 :: p3 :: p4 :: p5 :: p6 :: p7 :: Nil => Tuple7(p1, p2, p3, p4, p5, p6, p7)
      case p1 :: p2 :: p3 :: p4 :: p5 :: p6 :: p7 :: p8 :: Nil => Tuple8(p1, p2, p3, p4, p5, p6, p7, p8)
      case p1 :: p2 :: p3 :: p4 :: p5 :: p6 :: p7 :: p8 :: p9 :: Nil => Tuple9(p1, p2, p3, p4, p5, p6, p7, p8, p9)
      case _ => throw new IllegalAccessException("Only Tuple1-9 are supported by TupleSerde")
    }
  }

  override def close() = ()
}
