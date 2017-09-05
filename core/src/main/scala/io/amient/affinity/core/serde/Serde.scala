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

import akka.actor.ActorSystem
import akka.serialization.{JSerializer, SerializationExtension, Serializer}

import scala.reflect.ClassTag

trait Serde[T] extends JSerializer with AbstractSerde[T] {

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = toBytes(obj.asInstanceOf[T])

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = fromBytes(bytes) match {
    case null => null
    case ref: AnyRef => ref
    case u: Unit => u.asInstanceOf[AnyRef]
    case z: Boolean => z.asInstanceOf[AnyRef]
    case b: Byte => b.asInstanceOf[AnyRef]
    case c: Char => c.asInstanceOf[AnyRef]
    case s: Short => s.asInstanceOf[AnyRef]
    case i: Int => i.asInstanceOf[AnyRef]
    case l: Long => l.asInstanceOf[AnyRef]
    case f: Float => f.asInstanceOf[AnyRef]
    case d: Double => d.asInstanceOf[AnyRef]
  }

}

object Serde {

  def of[S: ClassTag](system: ActorSystem): AbstractSerde[S] = {
    val cls = implicitly[ClassTag[S]].runtimeClass
    SerializationExtension(system).serializerFor(serdeClass(cls)) match {
      case as: AbstractSerde[_] => as.asInstanceOf[AbstractSerde[S]]
      case s: Serializer => new AbstractSerde[S] {
        override def fromBytes(bytes: Array[Byte]) = s.fromBinary(bytes).asInstanceOf[S]

        override def toBytes(obj: S) = s.toBinary(obj.asInstanceOf[AnyRef])

        override def close() = ()
      }
    }
  }

  private def serdeClass(cls: Class[_]) = {
    if (cls == classOf[Boolean]) classOf[java.lang.Boolean]
    else if (cls == classOf[Byte]) classOf[java.lang.Byte]
    else if (cls == classOf[Int]) classOf[java.lang.Integer]
    else if (cls == classOf[Long]) classOf[java.lang.Long]
    else if (cls == classOf[Float]) classOf[java.lang.Float]
    else if (cls == classOf[Double]) classOf[java.lang.Double]
    else cls
  }

}
