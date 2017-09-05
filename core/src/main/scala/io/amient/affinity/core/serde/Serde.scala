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
import akka.serialization._

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
    val cls = serdeClass(implicitly[ClassTag[S]].runtimeClass)
    val ser = SerializationExtension(system).serializerFor(cls)
    toAbstractSerde[S](ser)
  }

  private def toAbstractSerde[S](serde: AnyRef) = serde match {
    case as: AbstractSerde[_] => as.asInstanceOf[AbstractSerde[S]]
    case s: Serializer => new AbstractSerde[S] {
      override def fromBytes(bytes: Array[Byte]) = s.fromBinary(bytes).asInstanceOf[S]

      override def toBytes(obj: S) = s.toBinary(obj.asInstanceOf[AnyRef])

      override def close() = ()
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

//TODO custom of method without using actdor system
//  private val serializerMap = new ConcurrentHashMap[Class[_], Serializer]()
//
//  def of[S: ClassTag](config: Config): AbstractSerde[S] = {
//
//    val clazz = serdeClass(implicitly[ClassTag[S]].runtimeClass)
//
//    val settings = new Serialization.Settings(config)
//
//    val bindings: immutable.Seq[ClassSerializer] = {
//      val fromConfig = for {
//        (className: String, alias: String) ← settings.SerializationBindings
//        if alias != "none" && checkGoogleProtobuf(className)
//      } yield (system.dynamicAccess.getClassFor[Any](className).get, serializers(alias))
//
//      val fromSettings = serializerDetails.flatMap { detail ⇒
//        detail.useFor.map(clazz ⇒ clazz → detail.serializer)
//      }
//
//      val result = sort(fromConfig ++ fromSettings)
//      ensureOnlyAllowedSerializers(result.map { case (_, ser) ⇒ ser }(collection.breakOut))
//      result
//    }
//
//    val ser = serializerMap.get(clazz) match {
//      case null ⇒ // bindings are ordered from most specific to least specific
//        def unique(possibilities: immutable.Seq[(Class[_], Serializer)]): Boolean =
//          possibilities.size == 1 ||
//            (possibilities forall (_._1 isAssignableFrom possibilities(0)._1)) ||
//            (possibilities forall (_._2 == possibilities(0)._2))
//
//        val ser: Serializer = {
//          bindings.filter {
//            case (c, _) ⇒ c isAssignableFrom clazz
//          } match {
//            case immutable.Seq() ⇒
//              throw new NotSerializableException("No configured serialization-bindings for class [%s]" format clazz.getName)
//            case possibilities ⇒
//              //possibilities(0)._2
//              throw new IllegalArgumentException("Multiple serializers found for " + clazz + ", choosing first: " + possibilities)
//
//          }
//        }
//        serializerMap.putIfAbsent(clazz, ser) match {
//          case null ⇒ ser
//          case some ⇒ some
//        }
//      case ser ⇒ ser
//    }
//    toAbstractSerde[S](ser)
//  }




}
