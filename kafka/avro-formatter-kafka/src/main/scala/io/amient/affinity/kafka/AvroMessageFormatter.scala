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

package io.amient.affinity.kafka

import java.io.PrintStream
import java.net.URL
import java.util.{Objects, Properties}

import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.{AvroJsonConverter, AvroSerde}
import io.amient.affinity.avro.{ConfluentSchemaRegistry, ZookeeperSchemaRegistry}
import io.amient.affinity.core.util.EventTime
import io.amient.affinity.kafka.AvroMessageFormatter.TimesstampToIso
import kafka.common.MessageFormatter
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.codehaus.jackson.map.ObjectMapper


/**
  * See README of this module for Usage in kafka console consumer utility.
  *
  */
class AvroMessageFormatter extends MessageFormatter {

  private var serde: AvroSerde = _

  private var pretty = false
  private var printKey = false
  private var printValue = true
  private var printOffset = false
  private var printPartition = false
  private var printTimestamps = false
  private var printType = false

  override def init(props: Properties): Unit = {
    if (props.containsKey("pretty")) pretty = true
    if (props.containsKey("print.key")) printKey = true
    if (props.containsKey("print.offset")) printOffset = true
    if (props.containsKey("print.timestamp")) printTimestamps = true
    if (props.containsKey("print.partition")) printPartition = true
    if (props.containsKey("print.type")) printType = true
    if (props.containsKey("no.value")) printValue = false
    if (props.containsKey("schema.registry.url")) {
      val conf = new CfAvroConf()
      conf.ConfluentSchemaRegistryUrl.setValue(new URL(props.getProperty("schema.registry.url")))
      serde = new ConfluentSchemaRegistry(conf)
    } else if (props.containsKey("schema.registry.zookeeper.connect")) {
      val conf = new ZkAvroConf()
      conf.ZooKeeper.Connect.setValue(props.getProperty("schema.registry.zookeeper.connect"))
      if (!props.containsKey("schema.registry.zookeeper.root")) {
        conf.ZkRoot.setValue(props.getProperty("schema.registry.zookeeper.root"))
      }
      serde = new ZookeeperSchemaRegistry(conf)
    } else {
      throw new IllegalArgumentException("Required --property schema.registry.url OR --property schema.zookeeper.connect")
    }
  }

  override def close(): Unit =
    if (Objects.nonNull(serde)) {
      serde.close()
      serde = null
    }

  val mapper = new ObjectMapper

  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    if (printPartition) output.printf("%s: ", consumerRecord.partition.toString)

    if (printOffset) output.printf("%s\t", consumerRecord.offset.toString)

    if (printTimestamps) output.printf("%s\t", consumerRecord.timestamp.iso)

    if (printKey) {
      val key = serde.fromBytes(consumerRecord.key)
      val simpleJson = AvroJsonConverter.toJson(key)
      output.printf("%s\t", simpleJson)
    }

    if (printValue) {
      val value = serde.fromBytes(consumerRecord.value)
      if (printType) {
        val `type` = value match {
          case container: GenericContainer => container.getSchema.getName
          case _ => value.getClass.getSimpleName
        }

        output.printf("%s: ", `type`)
      }

      val simpleJson = AvroJsonConverter.toJson(value)
      if (pretty) {
        val json = mapper.readTree(simpleJson)
        output.append(mapper.writerWithDefaultPrettyPrinter.writeValueAsString(json))
      } else {
        output.append(simpleJson)
      }
    }

    val nonBlankLine = printPartition || printOffset || printTimestamps || printKey || printValue

    if (nonBlankLine) output.println else output.flush
  }
}

object AvroMessageFormatter {

  implicit class TimesstampToIso(val ts: Long) extends AnyVal {
    def iso: String = EventTime.local(ts).toString
  }

}
