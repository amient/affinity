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

import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import io.amient.affinity.avro.util.ZooKeeperClient
import org.I0Itec.zkclient.IZkChildListener
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.collection.immutable

object ZkAvroSchemaRegistry {
  final val CONFIG_ZOOKEEPER_ROOT = "affinity.avro.zookeeper-schema-registry.zookeeper.root"
  final val CONFIG_ZOOKEEPER_CONNECT = "affinity.avro.zookeeper-schema-registry.zookeeper.connect"
  final val CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS = "affinity.avro.zookeeper-schema-registry.zookeeper.timeout.connect.ms"
  final val CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS = "affinity.avro.zookeeper-schema-registry.zookeeper.timeout.session.ms"
}

class ZkAvroSchemaRegistry(config: Config) extends AvroSerde with AvroSchemaProvider {

  val zkConnect = config.getString(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT)
  val zkSessionTimeout = config.getInt(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS)
  val zkConnectTimeout = config.getInt(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS)

  private val zkRoot = config.getString(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_ROOT)

  private val zk = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectTimeout)

  @volatile private var internal = immutable.Map[String, List[(Int, Schema)]]()

  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)
  updateInternal(zk.subscribeChildChanges(zkRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateInternal(children)
    }
  }))

  override def close(): Unit = zk.close()

  override private[schema] def registerSchema(cls: Class[_], schema: Schema): Int = {
    val path = zk.create(s"$zkRoot/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
    val id = path.substring(zkRoot.length + 1).toInt
    id
  }

  override private[schema] def getAllRegistered: List[(Int, Schema)] = {
    val ids = zk.getChildren(zkRoot)
    ids.asScala.toList.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val schemaId = id.toInt
      (schemaId, schema)
    }
  }

  override private[schema] def hypersynchronized[X](f: => X): X = synchronized {
    val lockPath = zkRoot + "-lock"
    var acquired = 0
    do {
      try {
        zk.createEphemeral(lockPath)
        acquired = 1
      } catch {
        case _: ZkNodeExistsException =>
          acquired -= 1
          if (acquired < -100) {
            throw new IllegalStateException("Could not acquire zk registry lock")
          } else {
            Thread.sleep(500)
          }
      }
    } while (acquired != 1)
    try f finally zk.delete(lockPath)
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
