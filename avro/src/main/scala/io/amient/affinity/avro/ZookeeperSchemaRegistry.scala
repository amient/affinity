/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.{CfgString, CfgStruct}
import io.amient.affinity.core.util.{ZkClients, ZkConf}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._

object ZookeeperSchemaRegistry {

  object ZkAvroConf extends ZkAvroConf {
    override def apply(config: Config) = new ZkAvroConf().apply(config)
  }

  class ZkAvroConf extends CfgStruct[ZkAvroConf](classOf[AvroConf]) {
    val ZooKeeper = struct("schema.registry.zookeeper", new ZkConf)
    val ZkRoot: CfgString = string("schema.registry.zookeeper.root", "/affinity-schema-registry")
  }
  
}


class ZookeeperSchemaRegistry(zkRoot: String, zk: ZkClient) extends AvroSerde with AvroSchemaRegistry {

  def this(conf: ZkAvroConf) = this(conf.ZkRoot(), {
    val zk = ZkClients.get(conf.ZooKeeper)
    val zkRoot = conf.ZkRoot()
    if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot)
    val zkSchemas = s"$zkRoot/schemas"
    if (!zk.exists(zkSchemas)) zk.createPersistent(zkSchemas)
    val zkSubjects = s"$zkRoot/subjects"
    if (!zk.exists(zkSubjects)) zk.createPersistent(zkSubjects)
    zk
  })

  def this(config: Config) = this {
    new ZkAvroConf().apply(config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path)))
  }

  override def close(): Unit = ZkClients.close(zk)

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = {
    val zkSchema = s"$zkRoot/schemas/${id.toString.reverse.padTo(10, '0').reverse}"
    if (!zk.exists(zkSchema)) throw new NoSuchElementException else
    new Schema.Parser().parse(zk.readData[String](zkSchema))
  }

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = hypersynchronized {
    val zkSubject = s"$zkRoot/subjects/$subject"
    val zkSchemas = s"$zkRoot/schemas"
    val versions: Map[Schema, Int] =
      if (!zk.exists(zkSubject)) Map.empty else {
        zk.readData[String](zkSubject) match {
          case some => some.split(",").toList.map(_.toInt).map {
            case id => getSchema(id) -> id
          }.toMap
        }
      }
    versions.get(schema).getOrElse {
      validator.validate(schema, versions.map(_._1).asJava)
      val schemaPath = zk.create(s"$zkSchemas/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
      val id = schemaPath .substring(zkSchemas.length + 1).toInt
      val updatedVersions = versions.map(_._2).toList :+ id
      if (zk.exists(zkSubject)) {
        zk.writeData(zkSubject, updatedVersions.mkString(","))
      } else {
        zk.create(zkSubject, updatedVersions.mkString(","), CreateMode.PERSISTENT)
      }
      id
    }
  }


  private def hypersynchronized[X](f: => X): X = synchronized {
    val lockPath = zkRoot + "/lock"
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

}
