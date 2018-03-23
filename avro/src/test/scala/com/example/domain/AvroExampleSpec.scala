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

package com.example.domain

import java.io.ByteArrayOutputStream

import io.amient.affinity.avro.record.AvroRecord
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.scalatest.{Matchers, WordSpec}

object Gender extends Enumeration {
  type Gender = Value
  val Male, Female = Value
}

case class Person(  name: String,
                    gender: Gender.Value,
                    score: Option[Double] = None) extends AvroRecord



class AvroExampleSpec extends WordSpec with Matchers {

  val person: Person = Person("Jeff Lebowski", Gender.Male, Some(11.5))

  val bytes = AvroRecord.write(person, person.getSchema, new ByteArrayOutputStream).toByteArray

  "AvroRecord global object provides some useful factory methods" in {
    /*val schema1 = */ AvroRecord.inferSchema[Person]
    /*val schema2 = */ AvroRecord.inferSchema(classOf[Person])
    /*val schema3 = */ AvroRecord.inferSchema("com.example.domain.Person")
  }

  "All of the schemas are equivalent and can be used in various scenarios\n" +
    "but an instance of this class also has all the methods of a Specific record\n" +
    ", including getSchema:" in {

    /*val schema = */person.getSchema
    println(person.getSchema)
  }

  "The above class can be converted to binary bytes with the standard Apache Avro tools:" in {
    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    val writer = new GenericDatumWriter[Any](person.schema)
    writer.write(person, encoder)
    encoder.flush()
    /*val bytes: Array[Byte] = */ output.toByteArray
  }

  "which is the same thing using the AvroRecord compation object method:" in {
    val output = new ByteArrayOutputStream()
    AvroRecord.write(person, person.getSchema, output)
    /*val bytes: Array[Byte] = */output.toByteArray
  }

  "or if you want Array[Byte] directly:" in {
    /*val bytes: Array[Byte] = */AvroRecord.write(person, person.getSchema)
  }

  "converting bytes back to case class requires a type tag" in {
    val readerSchema = AvroRecord.inferSchema[Person]
    /*val person: Person = */AvroRecord.read[Person](bytes, readerSchema)
  }

}
