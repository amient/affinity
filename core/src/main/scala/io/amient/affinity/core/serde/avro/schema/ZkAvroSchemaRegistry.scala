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

package io.amient.affinity.core.serde.avro.schema
import java.util

import io.amient.affinity.core.serde.avro.AvroSerde
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

import scala.collection.immutable
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._

class ZkAvroSchemaRegistry(zk: ZkClient) extends AvroSerde with AvroSchemaProvider {

  val zkRoot = "/schema-registry"
  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)
  updateRegistry(zk.subscribeChildChanges(zkRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateRegistry(children)
    }
  }))

  private val typeCache = mutable.Map[Class[_], Type]()

  private var register = immutable.Map[Schema, (Int, Class[_], Type)]()

  private def updateRegistry(ids: util.List[String]) = synchronized {
    register = ids.asScala.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val schemaId = id.toInt
      val schemaClass = Class.forName(schema.getFullName)
      val schemaType = typeCache(schemaClass)
      schema -> (schemaId, schemaClass, schemaType)
    }.toMap
  }

  override protected def registerType(tpe: Type, cls: Class[_], schema: Schema): Int = synchronized {
    register.get(schema) match {
      case Some((id2, cls2, tpe2)) if (cls2 == cls) => throw new IllegalArgumentException("Same class is already registered with that schema")
      case _ =>
        typeCache += cls -> tpe
        val path = zk.create(s"$zkRoot/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
        val id = path.substring(zkRoot.length+1).toInt
        register += schema -> (id, cls, tpe)
        id
    }
  }

  override def getAllSchemas(): List[(Int, Schema, Class[_], Type)] = synchronized {
    register.toList.map {
      case (schema, (id, cls, tpe)) => (id, schema, cls, tpe)
    }
  }
}
