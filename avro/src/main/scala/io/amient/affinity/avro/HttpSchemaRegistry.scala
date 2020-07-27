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

import java.io.{File, FileInputStream}
import java.net.URL
import java.security.KeyStore

import com.typesafe.config.Config
import io.amient.affinity.avro.HttpSchemaRegistry.HttpAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.CfgStruct
import org.apache.avro.Schema
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._

object HttpSchemaRegistry {

  object HttpAvroConf extends HttpAvroConf {
    override def apply(config: Config): HttpAvroConf = new HttpAvroConf().apply(config)
  }

  class HttpAvroConf extends CfgStruct[HttpAvroConf](classOf[AvroConf]) {
    val HttpSchemaRegistryUrl = url("schema.registry.url", new URL("http://localhost:8081"))
      .doc("Http Schema Registry connection base URL")
    val HttpSchemaRegistryKeyStore = string("schema.registry.keystore", false).doc("optional <key-store-file>:<key-store-password>")
    val HttpSchemaRegistryTrustStore = string("schema.registry.truststore", false).doc("optional <trust-store-file>:<trust-store-password>")
  }

}


class HttpSchemaRegistry(client: HttpSchemaRegistryClient) extends AvroSerde with AvroSchemaRegistry {

  def this(conf: HttpAvroConf) = this(new HttpSchemaRegistryClient(
    conf.HttpSchemaRegistryUrl(),
    if (!conf.HttpSchemaRegistryKeyStore.isDefined) null else conf.HttpSchemaRegistryKeyStore(),
    if (!conf.HttpSchemaRegistryTrustStore.isDefined) null else conf.HttpSchemaRegistryTrustStore()))

  def this(_conf: AvroConf) = this(HttpAvroConf(_conf))

  override def close(): Unit = ()

  override protected def loadSchema(id: Int): Schema = client.getSchema(id)

  override protected def registerSchema(subject: String, schema: Schema): Int = {
    client.registerSchema(subject, schema)
  }

}

class HttpSchemaRegistryClient(baseUrl: URL, keyStoreP12: String = null, trustStoreP12: String = null) {

  private val mapper = new ObjectMapper

  val httpClient = baseUrl.getProtocol match {
    case "https" =>
      val sslContext = SSLContexts.custom()
      if (keyStoreP12 != null) {
        val Array(keyStoreFile, keyStorePass) = keyStoreP12.split(":")
        val keyStore = KeyStore.getInstance("PKCS12")
        keyStore.load(new FileInputStream(keyStoreFile), keyStorePass.toCharArray())
        sslContext.loadKeyMaterial(keyStore, keyStorePass.toCharArray)
      }
      if (trustStoreP12 != null) {
        val Array(trustStoreFile, trustStorePass) = trustStoreP12.split(":")
        sslContext.loadTrustMaterial(new File(trustStoreFile), trustStorePass.toCharArray)
      }
      HttpClients.custom().setSSLContext(sslContext.build()).build()
    case _ => HttpClients.createDefault()
  }

  def getSubjects: Iterator[String] = {
    val j = mapper.readValue(get("/subjects"), classOf[JsonNode])
    if (!j.has("error_code")) {
      j.elements().asScala.map(_.asText)
    } else {
      if (j.get("error_code").asInt == 40401) {
        Iterator.empty
      } else {
        throw new RuntimeException(j.get("message").asText)
      }
    }
  }

  def getVersions(subject: String): Iterator[Int] = {
    val j = mapper.readValue(get(s"/subjects/$subject/versions"), classOf[JsonNode])
    if (!j.has("error_code")) {
      j.elements().asScala.map(_.asInt)
    } else {
      if (j.get("error_code").asInt == 40401) {
        Iterator.empty
      } else {
        throw new RuntimeException(j.get("message").asText)
      }
    }
  }

  def getSchema(subject: String, version: Int): (Int, Schema) = {
    val j = mapper.readValue(get(s"/subjects/$subject/versions/$version"), classOf[JsonNode])
    if (j.has("error_code")) throw new RuntimeException(j.get("message").asText)
    (j.get("id").asInt, new Schema.Parser().parse(j.get("schema").asText))
  }

  def getSchema(id: Int): Schema = {
    val j = mapper.readValue(get(s"/schemas/ids/$id"), classOf[JsonNode])
    if (j.has("error_code")) throw new RuntimeException(j.get("message").asText)
    new Schema.Parser().parse(j.get("schema").asText)
  }

  def registerSchema(subject: String, schema: Schema): Int = {
    val entity = mapper.createObjectNode()
    entity.put("schema", schema.toString)
    val response = post(s"/subjects/$subject/versions", entity.toString)
    try {
      val j = mapper.readValue(response, classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").asText)
      if (j.has("id")) j.get("id").asInt else throw new IllegalArgumentException
    } catch {
      case e: Throwable => throw new RuntimeException(
        s"schema registration failed subject: ${schema.getFullName}, schema: ${schema}, response: $response", e)
    }

  }

  private def get(path: String): String = {
    val httpGet = httpClient.execute(new HttpGet(baseUrl + path))
    try {
      scala.io.Source.fromInputStream(httpGet.getEntity().getContent).mkString
    } finally {
      httpGet.close();
    }
  }

  private def post(path: String, entity: String): String = {
    val post = new HttpPost(baseUrl + path)
    post.addHeader("Content-Type", "application/json")
    post.addHeader("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
    post.setEntity(new StringEntity(entity))
    val response = httpClient.execute(post)
    try {
      scala.io.Source.fromInputStream(response.getEntity().getContent).mkString
    } finally {
      response.close();
    }
  }

}

