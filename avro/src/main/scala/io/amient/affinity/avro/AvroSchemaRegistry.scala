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

import io.amient.affinity.avro.record.AvroRecord
import org.apache.avro.Schema

import scala.collection.immutable
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
  * This trait ensures that all the getters are fast and thread-safe.
  * Implementing classes do not have to be thread-safe
  */
trait AvroSchemaRegistry {

  private var cacheByFqn = immutable.Map[String, (Int, Schema)]() // schema.fqn -> current schema id

  private var cacheById: immutable.Map[Int, Schema] = Map() // schema id -> schema

  private var cacheBySchema: immutable.Map[Schema, Int] = Map() // schema id -> schema

  private var cacheBySubject: immutable.Map[String, Map[Int, Schema]] = Map() // subject -> schema ids

  private[avro] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int

  private[avro] def getAllRegistered: List[(Int, String, Schema)]

  private[avro] def hypersynchronized[X](f: => X): X

  private val registration = ListBuffer[(String, Schema)]()

  register[Null]("null")
  register[Boolean]("boolean")
  register[Int]("int")
  register[Long]("long")
  register[Float]("float")
  register[Double]("double")
  register[String]("string")
  register[Array[Byte]]("bytes")

  def describeSchemas: Map[Int, Schema] = cacheById

  final def getVersions(subject: String): Option[Map[Int, Schema]] = {
    cacheBySubject.get(subject)
  }

  /**
    * Get current current compile-time schema for the given fully qualified type name
    *
    * @param fqn fuly qualified name of the type
    * @return
    */
  final def getCurrentSchema(fqn: String): Option[(Int, Schema)] = cacheByFqn.get(fqn)

  /**
    * Get schema for a given subject and schema id.
    * @param subject
    * @param schemaId
    * @return Schema
    */
  final def getSchema(subject: String, schemaId: Int): Option[Schema] = {
    cacheBySubject.get(subject).flatMap(_.get(schemaId))
  }

  /**
    * Get id of a given schema as registered by the underlying registry
    *
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

  //register compile-time type with a specific schema version and its fqn as subject
  final def register[T: TypeTag](schema: Schema): Unit = register(typeOf[T].typeSymbol.fullName, schema)

  //register compile-time type with its current/actual schema and a custom subject
  final def register[T: TypeTag](subject: String): Unit = register(subject, AvroRecord.inferSchema(typeOf[T]))

  //register compile-time type with its current/actual schema and its fqn as subject
  final def register[T: TypeTag]: Unit = register(typeOf[T].typeSymbol.fullName, AvroRecord.inferSchema(typeOf[T]))

  final def register(subject: String, schema: Schema) = {
    registration += ((subject, schema))
  }

  final def initialize(): List[Int] = initialize(None)

  final def initialize[K: ClassTag](subject: String): List[Int] = {
    val cls = implicitly[reflect.ClassTag[K]].runtimeClass
    val mirror = universe.runtimeMirror(cls.getClassLoader)
    val keyType = mirror.classSymbol(implicitly[reflect.ClassTag[K]].runtimeClass).toType
    val keySchema = AvroRecord.inferSchema(keyType)
    initialize(subject, keySchema)
  }

  final def initialize(subject: String, schema: Schema): List[Int] = initialize(Some(subject, schema))

  private def initialize(onDemandRegister: Option[(String, Schema)]): List[Int] = hypersynchronized {
    onDemandRegister.foreach {
      case (onDemandSubject, onDemandSchema) => register(onDemandSubject, onDemandSchema)
    }
    val all: List[(Int, String, Schema)] = getAllRegistered
    all.foreach { case (id, subject, schema) =>
      cacheById += id -> schema
      cacheBySchema += schema -> id
      cacheBySubject += subject -> (cacheBySubject.get(subject).getOrElse(ListMap[Int, Schema]()) + (id -> schema))
    }
    val executableRegistration = registration.result()
    registration.clear()

    val result = ListBuffer[Int]()

    executableRegistration.foreach {
      case (subject, schema) =>
        val alreadyRegisteredId: Int = cacheBySchema.get(schema).getOrElse(-1)
        val versions = getVersions(subject).getOrElse(ListMap()).toList
        val schemaId = if ((alreadyRegisteredId == -1) || (!versions.contains((alreadyRegisteredId, schema)))) {
          val id = registerSchema(subject, schema, versions.map(_._2))
          cacheBySchema += schema -> id
          cacheById += id -> schema
          cacheBySubject += subject -> (cacheBySubject.get(subject).getOrElse(ListMap.empty[Int, Schema]) + (id -> schema))
          id
        } else {
          alreadyRegisteredId
        }
        result += schemaId
    }

    cacheBySchema.keys.map(_.getFullName).foreach { fqn =>
      try {
        val schema = AvroRecord.inferSchema(fqn)
        cacheBySchema.get(schema) match {
          case Some(schemaId) => cacheByFqn += fqn -> (schemaId, schema)
          case None =>
            val versions = cacheBySubject.get(fqn).getOrElse(ListMap.empty[Int, Schema])
            val schemaId = registerSchema(fqn, schema, versions.toList.map(_._2))
            cacheById += schemaId -> schema
            cacheBySchema += schema -> schemaId
            cacheBySubject += fqn -> (versions + (schemaId -> schema))
            cacheByFqn += fqn -> (schemaId, schema)
        }
      } catch {
        case _: java.lang.ClassNotFoundException => //class doesn't exist in the current runtime - not a problem, we'll use generic records
      }
    }
    result.result()
  }



}
