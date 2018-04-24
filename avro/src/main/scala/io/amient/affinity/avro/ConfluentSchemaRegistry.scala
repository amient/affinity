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

import java.io.DataOutputStream
import java.net.{HttpURLConnection, URL}

import com.typesafe.config.Config
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.CfgStruct
import org.apache.avro.Schema
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._

object ConfluentSchemaRegistry {

  object CfAvroConf extends CfAvroConf {
    override def apply(config: Config) = new CfAvroConf().apply(config)
  }

  class CfAvroConf extends CfgStruct[CfAvroConf](classOf[AvroConf]) {
    val ConfluentSchemaRegistryUrl = url("schema.registry.url", new URL("http://localhost:8081"))
      .doc("Confluent Schema Registry connection base URL")
  }

}


class ConfluentSchemaRegistry(client: ConfluentSchemaRegistryClient) extends AvroSerde with AvroSchemaRegistry {

  def this(config: Config) = this {
    val conf = CfAvroConf(config)
    new ConfluentSchemaRegistryClient(conf.ConfluentSchemaRegistryUrl())
  }

  override def close(): Unit = ()

  override protected def loadSchema(id: Int): Schema = client.getSchema(id)

  override protected def registerSchema(subject: String, schema: Schema): Int = {
    client.registerSchema(subject, schema)
  }

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

  def registerSchema(subject: String, schema: Schema): Int = {
    val entity = mapper.createObjectNode()
    entity.put("schema", schema.toString)
    val j = mapper.readValue(post(s"/subjects/$subject/versions", entity.toString), classOf[JsonNode])
    if (j.has("error_code")) throw new RuntimeException(subject + ": " + schema.getFullName + " - " + j.get("message").getTextValue)
    if (j.has("id")) j.get("id").getIntValue else throw new IllegalArgumentException
  }

  private def get(path: String): String = http(path) { connection =>
    try {
      connection.setRequestMethod("GET")
    } catch {
      case _: java.net.ConnectException => throw new java.net.ConnectException(baseUrl + path)
    }
  }

  private def post(path: String, entity: String): String = http(path) { connection =>
    connection.addRequestProperty("Content-Type", "application/json")
    connection.addRequestProperty("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
    connection.setDoOutput(true)
    connection.setRequestMethod("POST")
    val output = try {
      new DataOutputStream( connection.getOutputStream())
    } catch {
      case _: java.net.ConnectException => throw new java.net.ConnectException(baseUrl + path)
    }
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

