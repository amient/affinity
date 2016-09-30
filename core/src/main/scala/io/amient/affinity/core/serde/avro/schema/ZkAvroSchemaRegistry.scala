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
import io.amient.affinity.core.cluster.CoordinatorZk
import io.amient.affinity.core.serde.avro.AvroSerde
import io.amient.affinity.core.util.ZooKeeperClient
import org.I0Itec.zkclient.IZkChildListener
import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.collection.immutable

class ZkAvroSchemaRegistry(system: ExtendedActorSystem) extends AvroSerde with AvroSchemaProvider {

  //TODO #16 separate configuration avro schemas in zookeeper - reusing here cooridnator's config - add zk root configuration
  import CoordinatorZk._

  private val config = system.settings.config
  private val zkConnect = config.getString(CONFIG_ZOOKEEPER_CONNECT)
  private val zkConnectTimeout = config.getInt(CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS)
  private val zkSessionTimeout = config.getInt(CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS)
  private val zkRoot = "/schema-registry"
  private val zk = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectTimeout)

  @volatile private var internal = immutable.Map[String, List[(Int, Schema)]]()

  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)
  updateInternal(zk.subscribeChildChanges(zkRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateInternal(children)
    }
  }))

  override def getSchema(id: Int): Option[Schema] = try {
    Some(new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id")))
  } catch {
    case e: Throwable => e.printStackTrace(); None
  }

  override private[schema] def registerSchema(cls: Class[_], schema: Schema): Int = {
    val path = zk.create(s"$zkRoot/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
    val id = path.substring(zkRoot.length + 1).toInt
    id
  }

  override private[schema] def getVersions(cls: Class[_]): List[(Int, Schema)] = {
    val FQN = cls.getName
    val ids = zk.getChildren(zkRoot)
    ids.asScala.toList.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val schemaId = id.toInt
      (schemaId, schema)
    } filter {
      _._2.getFullName == FQN
    }
  }

  private def updateInternal(ids: util.List[String]): Unit = {
    internal = ids.asScala.toList.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val FQN = schema.getFullName
      val schemaId = id.toInt
      (FQN, (schemaId, schema))
    }.groupBy(_._1).mapValues(_.map(_._2))
  }

}
