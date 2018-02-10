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

import java.io.DataOutputStream
import java.net.{HttpURLConnection, URL}

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.apache.avro.Schema
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._

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

  class ConfluentSchemaRegistryClient(baseUrl: URL) {

    private val mapper = new ObjectMapper

    def getSubjects: Iterator[String] = {
      val j = mapper.readValue(get("/subjects"), classOf[JsonNode])
      if (!j.has("error_code")) {
        j.getElements().asScala.map(_.getTextValue)
      } else {
        if (j.get("error_code").getIntValue == 40401) {
          Iterator.empty
        } else {
          throw new RuntimeException(j.get("message").getTextValue)
        }
      }
    }

    def getVersions(subject: String): Iterator[Int] = {
      val j = mapper.readValue(get(s"/subjects/$subject/versions"), classOf[JsonNode])
      if (!j.has("error_code")) {
        j.getElements().asScala.map(_.getIntValue)
      } else {
        if (j.get("error_code").getIntValue == 40401) {
          Iterator.empty
        } else {
          throw new RuntimeException(j.get("message").getTextValue)
        }
      }
    }

    def getSchema(subject: String, version: Int): (Int, Schema) = {
      val j = mapper.readValue(get(s"/subjects/$subject/versions/$version"), classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").getTextValue)
      (j.get("id").getIntValue, new Schema.Parser().parse(j.get("schema").getTextValue))
    }

    def getSchema(id: Int): Schema = {
      val j = mapper.readValue(get(s"/schemas/ids/$id"), classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").getTextValue)
      new Schema.Parser().parse(j.get("schema").getTextValue)
    }

    def checkSchema(subject: String, schema: Schema): Option[Int] = {
      val entity = mapper.createObjectNode()
      entity.put("schema", schema.toString)
      val j = mapper.readValue(post(s"/subjects/$subject", entity.toString), classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").getTextValue)
      if (j.has("id")) Some(j.get("id").getIntValue) else None
    }

    def registerSchema(subject: String, schema: Schema): Int = {
      val entity = mapper.createObjectNode()
      entity.put("schema", schema.toString)
      val j = mapper.readValue(post(s"/subjects/$subject/versions", entity.toString), classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(subject + ": " + schema.getFullName + " - " + j.get("message").getTextValue)
      if (j.has("id")) j.get("id").getIntValue else throw new IllegalArgumentException
    }

    private def get(path: String): String = http(path) { connection =>
      connection.setRequestMethod("GET")
    }

    private def post(path: String, entity: String): String = http(path) { connection =>
      connection.addRequestProperty("Content-Type", "application/json")
      connection.addRequestProperty("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
      connection.setDoOutput(true)
      connection.setRequestMethod("POST")
      val output = new DataOutputStream( connection.getOutputStream())
      output.write( entity.getBytes("UTF-8"))
    }

    private def http(path: String)(init: HttpURLConnection => Unit ): String = {
      val url = new URL(baseUrl.toString + path)
      val connection = url.openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)

      init(connection)

      val status = connection.getResponseCode()
      val inputStream = if (status == HttpURLConnection.HTTP_OK) {
        connection.getInputStream
      } else {
        connection.getErrorStream
      }
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close
      content
    }
  }

  override private[avro] def hypersynchronized[X](f: => X) = synchronized {
    //TODO: verify that confluent schema registry behaves well under concurrent attempts to register the same schema
    f
  }
}
