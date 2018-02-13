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

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.avro.util.ConfluentSchemaRegistryClient
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.apache.avro.Schema

object ConfluentSchemaRegistry {

  object Conf extends Conf {
    override def apply(config: Config): Conf = new Conf().apply(config)
  }

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Avro = struct("affinity.avro", new CfAvroConf, false)
  }

  class CfAvroConf extends CfgStruct[CfAvroConf](classOf[AvroConf]) {
    val ConfluentSchemaRegistryUrl= url("schema.registry.url", true)
  }

}

/**
  * Confluent Schema Registry serde
  * This uses Confluent Schema Registry but doesn't use the topic-key and topic-value subjects.
  * Instead a fully-qualified name of the class is the subject.
  */
class ConfluentSchemaRegistry(config: Config) extends AvroSerde with AvroSchemaRegistry {
  val merged = config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path))
  val conf = new CfAvroConf().apply(merged)
  val client = new ConfluentSchemaRegistryClient(conf.ConfluentSchemaRegistryUrl())

  override def close(): Unit = ()

  override private[avro] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int = {
    client.registerSchema(subject, schema)
  }

  override private[avro] def getAllRegistered: List[(Int, String, Schema)] = {
    client.getSubjects.flatMap {
      subject =>
        client.getVersions(subject).toList.map { version =>
          client.getSchema(subject, version) match {
            case (id, schema) => (id, subject, schema)
          }
        }
    }.toList
  }

  override private[avro] def hypersynchronized[X](f: => X) = synchronized {
    //TODO: verify that confluent schema registry behaves well under concurrent attempts to register the same schema
    f
  }
}
