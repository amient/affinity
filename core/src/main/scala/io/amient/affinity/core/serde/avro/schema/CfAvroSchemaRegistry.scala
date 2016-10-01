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


import akka.actor.ExtendedActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.amient.affinity.core.serde.avro.AvroSerde
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object CfAvroSchemaRegistry {
  final val CONFIG_CF_REGISTRY_URL_BASE = "affinity.confluent-schema-registry.url.base"
}
/**
  * Confluent Schema Registry provider and serde
  * This provider uses Confluent Schema Registry but doesn't use the topic-key and topic-value subjects.
  * Instead a fully-qualified name of the class is the subject.
  * TODO #16 test that the confluent deserializer works out-of-box - the subject should not be used only schema id
  * TODO #16 provide a wrapper for the confluent serializer which uses FQN for subjects
  */
class CfAvroSchemaRegistry(system: ExtendedActorSystem) extends AvroSerde with AvroSchemaProvider {

  import CfAvroSchemaRegistry._
  val config = system.settings.config
  val client = new ConfluentSchemaRegistryClient(Uri(config.getString(CONFIG_CF_REGISTRY_URL_BASE)))

  override private[schema] def getSchema(id: Int): Option[Schema] = try {
    Some(client.getSchema(id))
  } catch {
    case e: Throwable => e.printStackTrace(); None
  }

  override private[schema] def registerSchema(cls: Class[_], schema: Schema): Int = {
    val subject = cls.getName
    client.registerSchema(subject, schema)
  }

  override private[schema] def getVersions(cls: Class[_]): List[(Int, Schema)] = {
    val subject = cls.getName
    client.getVersions(subject).toList.map { version =>
      client.getSchema(subject, version)
    }
  }

  class ConfluentSchemaRegistryClient(baseUrl: Uri) {
    implicit val system = CfAvroSchemaRegistry.this.system

    import system.dispatcher

    implicit val materializer = ActorMaterializer.create(system)

    private val mapper = new ObjectMapper

    def getSubjects: Iterator[String] = {
      val j = mapper.readValue(http(GET, "/subjects").utf8String, classOf[JsonNode])
      if (!j.has("error_code")) {
        j.elements().asScala.map(_.textValue())
      } else {
        if (j.get("error_code").intValue() == 40401) {
          Iterator.empty
        } else {
          throw new RuntimeException(j.get("message").textValue())
        }
      }
    }

    def getVersions(subject: String): Iterator[Int] = {
      val j = mapper.readValue(http(GET, s"/subjects/$subject/versions").utf8String, classOf[JsonNode])
      if (!j.has("error_code")) {
        j.elements().asScala.map(_.intValue())
      } else {
        if (j.get("error_code").intValue() == 40401) {
          Iterator.empty
        } else {
          throw new RuntimeException(j.get("message").textValue())
        }
      }
    }

    def getSchema(subject: String, version: Int): (Int, Schema) = {
      val j = mapper.readValue(http(GET, s"/subjects/$subject/versions/$version").utf8String, classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").textValue())
      (j.get("id").intValue(), new Schema.Parser().parse(j.get("schema").textValue()))
    }

    def getSchema(id: Int) = {
      val j = mapper.readValue(http(GET, s"/schemas/ids/$id").utf8String, classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").textValue())
      new Schema.Parser().parse(j.get("schema").textValue())
    }

    def checkSchema(subject: String, schema: Schema): Option[Int] = {
      val entity = mapper.createObjectNode()
      entity.put("schema", schema.toString)
      val j = mapper.readValue(http(POST, s"/subjects/$subject", entity.toString).utf8String, classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").textValue())
      if (j.has("id")) Some(j.get("id").intValue()) else None
    }

    def registerSchema(subject: String, schema: Schema): Int = {
      val entity = mapper.createObjectNode()
      entity.put("schema", schema.toString)
      val j = mapper.readValue(http(POST, s"/subjects/$subject/versions", entity.toString).utf8String, classOf[JsonNode])
      if (j.has("error_code")) throw new RuntimeException(j.get("message").textValue())
      if (j.has("id")) j.get("id").intValue() else throw new IllegalArgumentException
    }

    private def http(method: HttpMethod, path: String, entity: String = null): ByteString = {
      val uri = baseUrl.withPath(Uri.Path(path))
      val t = 5 seconds
      val req = if (entity == null) HttpRequest(method = method, uri = uri)
      else
        HttpRequest(method = method, uri = uri, entity = HttpEntity(ContentTypes.`application/json`, entity))
      val f: Future[HttpResponse] = Http().singleRequest(req).flatMap(_.toStrict(t))
      Await.result(f.flatMap(_.entity.toStrict(t)), t).data
    }

  }

}
