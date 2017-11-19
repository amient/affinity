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

package io.amient.affinity.avro.schema

import io.amient.affinity.avro.AvroRecord
import org.apache.avro.Schema

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

/**
  * This trait ensures that all the getters are fast and thread-safe.
  * Implementing classes do not have to be thread-safe
  */
trait AvroSchemaProvider {

  private var cacheByFqn = immutable.Map[String, (Int, Schema)]() // schema.fqn -> current schema id

  private var cacheById: immutable.Map[Int, Schema] = Map() // schema id -> schema

  private var cacheBySchema: immutable.Map[Schema, Int] = Map() // schema id -> schema

  private var cacheBySubject: immutable.Map[String, List[(Int, Schema)]] = Map() // subject -> schema ids

  private[schema] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int

  private[schema] def getAllRegistered: List[(Int, String, Schema)]

  private[schema] def hypersynchronized[X](f: => X): X

  private val registration = ListBuffer[(String, Schema)]()

  register[Null]("null")
  register[Boolean]("boolean")
  register[Int]("int")
  register[Long]("long")
  register[Float]("float")
  register[Double]("double")
  register[String]("string")
  //register[java.nio.ByteBuffer]("bytes")

  def describeSchemas: Map[Int, Schema] = cacheById

  /**
    * Get current current compile-time schema for the given fully qualified type name
    *
    * @param fqn fuly qualified name of the type
    * @return
    */
  final def getCurrentSchema(fqn: String): Option[(Int, Schema)] = cacheByFqn.get(fqn)

  /**
    * Get id of a given schema as registered by the underlying registry
    * @param schema
    * @return
    */
  final def getSchemaId(schema: Schema): Option[Int] = cacheBySchema.get(schema)

  /**
    * Get Type and Schema by a schema Id.
    * Newly registered type versions by other instances may not be associated
    * with any runtime type in this instance but their schemas will be available in
    * the shared external schema registry.
    *
    * @param id schema id
    * @return
    */
  final def schema(id: Int): Option[Schema] = {
    cacheById.get(id) match {
      case Some(existing) => Some(existing)
      case None =>
        initialize()
        cacheById.get(id) match {
          case Some(existing) => Some(existing)
          case None =>
            throw new NoSuchElementException(s"Schema with id $id is not recognized")
        }
    }
  }

  final def initialize(): List[Int] = hypersynchronized {
    val all: List[(Int, String, Schema)] = getAllRegistered
    all.foreach { case (id, subject, schema) =>
      cacheById += id -> schema
      cacheBySchema += schema -> id
      cacheBySubject += subject -> (cacheBySubject.get(subject).getOrElse(List()) :+ (id, schema))
    }
    val result = ListBuffer[Int]()
    val _register = mutable.HashSet[(Int, Schema, String)]()
    registration.result.foreach {
      case (subject, schema) =>
        val versions = cacheBySubject.get(subject).getOrElse(List())
        val alreadyRegisteredId: Int = (versions.map { case (id2, schema2) =>
          _register  += ((id2, schema2, subject))
          if (schema2 == schema) id2 else -1
        } :+ (-1)).max
        val schemaId = if (alreadyRegisteredId == -1) {
          val newlyRegisteredId = registerSchema(subject, schema, versions.map(_._2))
          _register  += ((newlyRegisteredId, schema, subject))
          newlyRegisteredId
        } else {
          alreadyRegisteredId
        }

        result += schemaId

        _register.foreach { case (id2, schema2, subject2) =>
          cacheById += id2 -> schema2
          cacheBySchema += schema2 -> id2
          cacheBySubject += subject2 -> (cacheBySubject.get(subject2).getOrElse(List()) :+ (id2, schema2))
        }
    }

    cacheBySchema.keys.map(_.getFullName).foreach { fqn =>
      try {
        val schema = AvroRecord.inferSchema(fqn)
        cacheByFqn += fqn -> (cacheBySchema(schema), schema)
      } catch {
        case NonFatal(e) => e.printStackTrace()
      }
    }

//    System.err.println("------------------------------------- cacheById:")
//    cacheById.toList.sortBy(_._1).foreach(System.err.println)
//    System.err.println("------------------------------------- cacheBySchema:")
//    cacheBySchema.toList.foreach(System.err.println)
//    System.err.println("------------------------------------- cacheBySubject:")
//    cacheBySubject.toList.sortBy(_._1).foreach(System.err.println)
//    System.err.println("-------------------------------------  cacheByFQN:")
//    cacheByFqn.toList.sortBy(_._1).foreach(System.err.println)
//    System.err.println("-------------------------------------")
    registration.clear()
    result.result()
  }


  //register compile-time type with a specific schema version and its fqn as subject
  final def register[T: TypeTag](schema: Schema): Unit = register(typeOf[T].typeSymbol.fullName, schema)

  //register compile-time type with its current/actual schema and a custom subject
  final def register[T: TypeTag](subject: String): Unit = register(subject, AvroRecord.inferSchema(typeOf[T]))

  //register compile-time type with its current/actual schema and its fqn as subject
  final def register[T: TypeTag]: Unit = register(typeOf[T].typeSymbol.fullName, AvroRecord.inferSchema(typeOf[T]))

  final def register(subject: String, schema: Schema) = {
    registration += ((subject, schema))
  }


}
