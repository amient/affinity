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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic._
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.DecoderFactory

import scala.collection.JavaConverters._

class AvroProjectionSpec extends FlatSpec with Matchers {

  val validator = new SchemaValidatorBuilder().mutualReadStrategy().validateAll() //equivalent to FULL_TRANSITIVE

  val schemaV1 = new Schema.Parser().parse("""
{
  "type":"record",
  "name":"SomeRecord",
  "namespace":"io.amient.affinity.avro",
  "fields":[
    { "name":"items",
      "default": [],
      "type":
      {
        "type":"array",
        "items":
        {
          "type":"record",
          "name":"SimpleRecord",
          "fields":
          [
            { "name":"id",
              "default":{"id":0},
              "type":{
                "type":"record",
                "name":"SimpleKey",
                "fields":[ { "name":"id", "type":"int"} ]}
            },
            { "name":"side",
              "default":"A",
              "type":
              {
                "type":"enum",
                "name":"SimpleEnum",
                "symbols":["A","B","C"]
              }
            },
            { "name":"seq",
              "default":[],
              "type":
              {
                "type":"array",
                "items":"SimpleKey"
              }
            }
          ]
        }
      }
    },
    { "name":"index",
      "default":{},
      "type":{
        "type":"map",
        "values":"SimpleRecord"
      }
    },
    { "name":"option",
       "default": { "id": { "id": 1 } },
       "type": [ "SimpleRecord", "null" ]
    },
    { "name": "deleted",
      "type" : "string",
      "default" : ""
    }
  ]
}""")

  val schemaV2 = new Schema.Parser().parse("""
{
  "type":"record",
  "name":"SomeRecord",
  "namespace":"io.amient.affinity.avro",
  "fields":[
    { "name":"items",
      "type":
      {
        "type":"array",
        "default":[],
        "items":
        {
          "type":"record",
          "name":"SimpleRecord",
          "fields":
          [
            { "name":"id",
              "default":{"id":0},
              "type":{
                "type":"record",
                "name":"SimpleKey",
                "fields":[ { "name":"id", "type":"int"} ]}
            },
            { "name":"sideRenamed",
              "aliases" : ["side"],
              "default":"A",
              "type":
              {
                "type":"enum",
                "name":"SimpleEnum",
                "symbols":["A","B","C"]
              }
            },
            { "name":"seq",
              "default":[],
              "type":
              {
                "type":"array",
                "items":"SimpleKey"
              }
            },
            { "name": "nestedAdded",
              "default": true,
              "type": "boolean"
            }
          ]
        }
      }
    },
    { "name":"index",
      "default":{},
      "type":{
        "type":"map",
        "values":"SimpleRecord"
      }
    },
    { "name":"optionRenamed",
      "aliases": [ "option" ],
      "default": { "id": { "id": 1 } },
      "type": [ "SimpleRecord", "null" ]
    },
    { "name": "added",
      "type" : "long",
      "default" : 0
    }
  ]
}""")

  "Projection rules " should "apply to nested schemas" in {
    val record = new GenericData.Record(schemaV1)
    val simple = new GenericData.Record(schemaV1.getField("option").schema().getTypes.get(0))
    val key = new GenericData.Record(schemaV1.getField("option").schema().getTypes.get(0).getField("id").schema())
    key.put("id", 100)
    simple.put("id", key)
    simple.put("seq", List(key).asJava)
    val sideSchema = schemaV1.getField("option").schema().getTypes.get(0).getField("side").schema()
    val side = new EnumSymbol(sideSchema, "B")
    simple.put("side", side)
    record.put("option", simple)
    record.put("items", List(simple).asJava)
    record.put("index", Map("1" -> simple).asJava) // new GenericData.Record implementation doesn't honour map default {}
    record.put("deleted", "") // new GenericData.Record implementation doesn't honour string default ""
    println(record)
    val writer = new GenericDatumWriter[AnyRef](schemaV1)
    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    writer.write(record, encoder)
    encoder.flush()
    val binary = output.toByteArray
    println(binary.mkString(" "))
    validator.validate(schemaV2, List(schemaV1).asJava)

    val input = new ByteArrayInputStream(binary)
    val decoder = DecoderFactory.get.binaryDecoder(input, null)
    val reader = new GenericDatumReader[GenericData.Record](schemaV1, schemaV2)
    val recordV2 = reader.read(null, decoder)
    println(recordV2)
    recordV2.get("optionRenamed").asInstanceOf[GenericData.Record].get("id") should equal(key)
    recordV2.get("optionRenamed").asInstanceOf[GenericData.Record].get("sideRenamed") should equal(side)
    recordV2.get("optionRenamed").asInstanceOf[GenericData.Record].get("nestedAdded") should equal(new java.lang.Boolean(true))
    recordV2.get("added") should be(0)
    recordV2.get("deleted") should be(null)

  }


}
