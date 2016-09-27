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

import akka.actor.ExtendedActorSystem
import io.amient.affinity.core.cluster.Node._
import io.amient.affinity.core.serde.avro.AvroSerde
import io.amient.affinity.core.util.ZooKeeperClient
import org.I0Itec.zkclient.IZkChildListener
import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe._

class ZkAvroSchemaRegistry(system: ExtendedActorSystem) extends AvroSerde with AvroSchemaProvider {

  private val config = system.settings.config
  private val zkConnect = config.getString(CONFIG_ZOOKEEPER_CONNECT)
  private val zkConnectTimeout = config.getInt(CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS)
  private val zkSessionTimeout = config.getInt(CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS)
  //TODO schmea registry zk root configuration
  private val zkRoot = "/schema-registry"
  private val zk = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectTimeout)
  private val typeCache = mutable.Map[Class[_], Type]()
  private var register = immutable.Map[Schema, (Int, Class[_], Type)]()

  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)
  updateRegistry(zk.subscribeChildChanges(zkRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateRegistry(children)
    }
  }))

  private def updateRegistry(ids: util.List[String]) = synchronized {
    register = ids.asScala.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val schemaId = id.toInt
      val schemaClass = Class.forName(schema.getFullName)
      val schemaType: Type = typeCache.get(schemaClass) match {
        case None => null
        case Some(tpe) => tpe
      }
      schema -> (schemaId, schemaClass, schemaType)
    }.toMap
  }

  override protected def registerType(tpe: Type, cls: Class[_], schema: Schema): Int = synchronized {
    register.get(schema) match {
      case Some((id2, cls2, tpe2)) if (cls2 == cls) =>
        //TODO this is a general behaviour of any schema registry
        register.foreach { case (s, (i, c, t)) =>
          if (c == cls && t != tpe) register += s -> (i, c, tpe)
        }
        id2
      case _ =>
        typeCache += cls -> tpe
        val path = zk.create(s"$zkRoot/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
        val id = path.substring(zkRoot.length + 1).toInt
        register += schema -> (id, cls, tpe)
        //TODO this is a general behaviour of any schema registry
        register.foreach { case (s, (i, c, t)) =>
          if (c == cls && t != tpe) register += s -> (i, c, tpe)
        }
        id
    }
  }

  override def getAllSchemas(): List[(Int, Schema, Class[_], Type)] = synchronized {
    register.toList.map {
      case (schema, (id, cls, tpe)) => (id, schema, cls, tpe)
    }
  }
}
