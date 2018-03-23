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

import java.io.NotSerializableException
import java.util.concurrent.ConcurrentHashMap

import akka.serialization._
import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.AffinityActorSystem
import io.amient.affinity.core.serde.primitive.ByteArraySerde

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
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

  def by[S](identity: Int, config: Config): Serde[S] = tools(config).by(identity).asInstanceOf[Serde[S]]

  def of[S: ClassTag](config: Config): AbstractSerde[S] = tools(config).of[S](config)

  def find(o: AnyRef, config: Config) = tools(config).find(o)

  private val _tools = new ConcurrentHashMap[Config, Serdes]()

  def tools(config: Config): Serdes = {
    _tools.get(config) match {
      case null => {
        val s = new Serdes(config
          .withFallback(AffinityActorSystem.defaultConfig)
          .withFallback(ConfigFactory.defaultReference))
        _tools.putIfAbsent(config, s) match {
          case null => s
          case some => some
        }
      }
      case some => some
    }
  }

}

class Serdes(val config: Config) {

  type ClassSerde = (Class[_], Serde[_])

  // com.google.protobuf serialization binding is only used if the class can be loaded,
  // i.e. com.google.protobuf dependency has been added in the application project.
  // The reason for this special case is for backwards compatibility so that we still can
  // include "com.google.protobuf.GeneratedMessage" = proto in configured serialization-bindings.
  def checkGoogleProtobuf(className: String): Boolean = (!className.startsWith("com.google.protobuf"))

  /**
    * Sort so that subtypes always precede their supertypes, but without
    * obeying any order between unrelated subtypes (insert sort).
    */
  def sort(in: Iterable[ClassSerde]): immutable.Seq[ClassSerde] =
    ((new ArrayBuffer[ClassSerde](in.size) /: in) { (buf, ca) ⇒
      buf.indexWhere(_._1 isAssignableFrom ca._1) match {
        case -1 ⇒ buf append ca
        case x ⇒ buf insert(x, ca)
      }
      buf
    }).to[immutable.Seq]

  private def serdeClass(cls: Class[_]) = {
    if (cls == classOf[Boolean]) classOf[java.lang.Boolean]
    else if (cls == classOf[Byte]) classOf[java.lang.Byte]
    else if (cls == classOf[Int]) classOf[java.lang.Integer]
    else if (cls == classOf[Long]) classOf[java.lang.Long]
    else if (cls == classOf[Float]) classOf[java.lang.Float]
    else if (cls == classOf[Double]) classOf[java.lang.Double]
    else cls
  }

  private val settings = new Serialization.Settings(config)

  private def serdeInstance(fqn: String): Option[Serde[_]] = {
    val cls = Class.forName(fqn)
    if (!(classOf[Serde[_]]).isAssignableFrom(cls))  {
      None
    } else {
      val clazz = cls.asSubclass(classOf[Serde[_]])
      try {
        Some(clazz.getConstructor(classOf[Serdes]).newInstance(this))
      } catch {
        case _: NoSuchMethodException =>
          try {
            Some(clazz.getConstructor().newInstance())
          } catch {
            case _: NoSuchMethodException => None
          }
      }
    }
  }

  private val serializers: Map[String, Serde[_]] = {
    (for ((k: String, v: String) ← settings.Serializers) yield {
      serdeInstance(v).map(x => k → x)
    }).flatten.toMap
  }

  def by(identifier: Int): Serde[_] = serializers.map(_._2).find(_.identifier == identifier)
    .getOrElse(throw new IllegalArgumentException(s"Serde not found, identifier: $identifier"))

  val bindings: immutable.Seq[ClassSerde] = {

    val fromConfig = for {
      (className: String, alias: String) ← settings.SerializationBindings
      if alias != "none" && checkGoogleProtobuf(className)
      if serializers.contains(alias)
    } yield (Class.forName(className), serializers(alias))

    val result = sort(fromConfig)
    result

  }

  def find(wrapped: AnyRef): Serde[_] = forClass(wrapped.getClass)

  def forClass(clazz: Class[_]): Serde[_] = {
    def unique(possibilities: immutable.Seq[(Class[_], Serde[_])]): Boolean =
      possibilities.size == 1 ||
        (possibilities forall (_._1 isAssignableFrom possibilities(0)._1)) ||
        (possibilities forall (_._2 == possibilities(0)._2))

    bindings.filter {
      case (c, _) ⇒ c isAssignableFrom clazz
    } match {
      case immutable.Seq() ⇒
        throw new NotSerializableException("No configured serialization-bindings for class [%s]" format clazz.getName)
      case possibilities ⇒
        if (!unique(possibilities))
          throw new IllegalArgumentException("Multiple serializers found for " + clazz + ", choosing first: " + possibilities)
        possibilities(0)._2
    }
  }

  private val serializerMap = new ConcurrentHashMap[Class[_], Serde[_]]()

  def of[S: ClassTag](config: Config): Serde[S] = {
    val runtimeClass = implicitly[ClassTag[S]].runtimeClass
    val ser = if (runtimeClass == classOf[Array[Byte]]) {
      new ByteArraySerde
    } else {
      serializerMap.get(runtimeClass) match {
        case null ⇒
          val ser = forClass(serdeClass(runtimeClass))
          serializerMap.putIfAbsent(runtimeClass, ser) match {
            case null ⇒ ser
            case some ⇒ some
          }

        case ser ⇒ ser
      }
    }
    ser.asInstanceOf[Serde[S]]
  }
}
