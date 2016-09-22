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

package io.amient.affinity.core.serde.avro

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.annotation.JsonIgnore
import io.amient.affinity.core.serde.avro.schema.AvroSchemaProvider
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, IndexedRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import org.apache.avro.{AvroRuntimeException, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object AvroRecord {

  private val typeSchemaCache = scala.collection.mutable.Map[Type, Schema]()

  private val cacheNameCache = scala.collection.mutable.Map[String, Class[_]]()

  private def classForName(fullClassName: String): Class[_] = {
    cacheNameCache.get(fullClassName) match {
      case Some(cls) => cls
      case None =>
        val cls = Class.forName(fullClassName)
        cacheNameCache.put(fullClassName, cls)
        cls
    }
  }

  def write(x: IndexedRecord, schemaId: Int): Array[Byte] = {
    write(x, x.getSchema, schemaId)
  }

  def write(x: Any, schema: Schema, schemaId: Int = -1): Array[Byte] = {
    val valueOut = new ByteArrayOutputStream()
    try {
      val encoder = EncoderFactory.get().binaryEncoder(valueOut, null)
      val writer = new GenericDatumWriter[Any](schema)
      if (schemaId >= 0) {
        encoder.writeInt(schemaId)
      }
      writer.write(x, encoder)
      encoder.flush()
      valueOut.toByteArray
    } finally {
      valueOut.close
    }
  }

  def read[T: TypeTag](bytes: Array[Byte], cls: Class[T], schema: Schema): T = read(bytes, cls, schema, schema)

  def read[T: TypeTag](bytes: Array[Byte], cls: Class[T], writerSchema: Schema, readerSchema: Schema): T = {
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
    read(bytes, cls, writerSchema, readerSchema, decoder)
  }

  def read(bytes: Array[Byte], schemaRegistry: AvroSchemaProvider): AnyRef = {
    if (bytes == null) null.asInstanceOf[AnyRef]
    else {
      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val schemaId = decoder.readInt()
      require(schemaId >= 0)
      val (tpe, cls, writerSchema) = schemaRegistry.schema(schemaId)
      val readerSchema = inferSchema(tpe)
      val reader = new GenericDatumReader[GenericRecord](writerSchema, readerSchema)
      val record: GenericRecord = reader.read(null, decoder)
      readRecord(record, cls)
    }
  }

  private def read[T: TypeTag](bytes: Array[Byte], cls: Class[T], writerSchema: Schema, readerSchema: Schema, decoder: BinaryDecoder): T = {
    val reader = new GenericDatumReader[GenericRecord](writerSchema, readerSchema)
    val record: GenericRecord = reader.read(null, decoder)
    readRecord(record, cls).asInstanceOf[T]
  }

  //TODO readDatum(record: GenericRecord, tpe: Type): AnyRef

  private def readRecord(record: GenericRecord, cls: Class[_]): AnyRef = {
    val c = cls.getConstructors()(0)
    val params = c.getParameterTypes
    val arguments: Seq[Object] = record.getSchema.getFields.asScala.map { field =>
      val datum: AnyRef = record.get(field.pos)
      val param: Class[_] = params(field.pos)
      readDatum(datum, param, field.schema)
    }
    c.newInstance(arguments: _*).asInstanceOf[AnyRef]
  }

  private def readDatum(datum: Any, cls: Class[_], schema: Schema): AnyRef = {
    schema.getType match {
      case BOOLEAN => new java.lang.Boolean(datum.asInstanceOf[Boolean])
      case INT => new java.lang.Integer(datum.asInstanceOf[Int])
      case NULL => null
      case FLOAT => new java.lang.Float(datum.asInstanceOf[Float])
      case DOUBLE => new java.lang.Double(datum.asInstanceOf[Double])
      case LONG => new java.lang.Long(datum.asInstanceOf[Long])
      case FIXED | BYTES => datum.asInstanceOf[Array[Byte]]
      case STRING => String.valueOf(datum.asInstanceOf[Utf8])
      case RECORD => readRecord(datum.asInstanceOf[GenericRecord], classForName(schema.getFullName))
      case ENUM => classForName(schema.getFullName).getMethod("withName", classOf[String])
        .invoke(null, datum.asInstanceOf[EnumSymbol].toString)
      case MAP => datum.asInstanceOf[java.util.Map[Utf8, _]].asScala.toMap
        .map { case (k, v) => (
          k.toString,
          readDatum(v, classForName(schema.getValueType.getFullName), schema.getValueType))
        }
      case ARRAY => val iterable = datum.asInstanceOf[java.util.Collection[_]].asScala
        .map(item => readDatum(item, classForName(schema.getElementType.getFullName), schema.getElementType))
        if (cls.isAssignableFrom(classOf[Set[_]])) {
          iterable.toSet
        } else if (cls.isAssignableFrom(classOf[List[_]])) {
          iterable.toList
        } else if (cls.isAssignableFrom(classOf[Vector[_]])) {
          iterable.toVector
        } else if (cls.isAssignableFrom(classOf[IndexedSeq[_]])) {
          iterable.toIndexedSeq
        } else if (cls.isAssignableFrom(classOf[Seq[_]])) {
          iterable.toSeq
        } else {
          iterable
        }
      case UNION => throw new NotImplementedError("Avro Unions are not supported")
    }
  }


  def inferSchema[X: TypeTag, AnyRef <: X](cls: Class[X]): Schema = inferSchema(typeOf[X])

  private def inferSchema(t: Type): Schema = {

    typeSchemaCache.get(t) match {
      case Some(schema) => schema
      case None =>

        val schema: Schema =
          if (t =:= typeOf[String]) {
            SchemaBuilder.builder().stringType()
          } else if (t =:= definitions.IntTpe) {
            SchemaBuilder.builder().intType()
          } else if (t =:= definitions.LongTpe) {
            SchemaBuilder.builder().longType()
          } else if (t =:= definitions.BooleanTpe) {
            SchemaBuilder.builder().booleanType()
          } else if (t =:= definitions.FloatTpe) {
            SchemaBuilder.builder().floatType()
          } else if (t =:= definitions.DoubleTpe) {
            SchemaBuilder.builder().doubleType()
          } else if (t <:< typeOf[Map[_, _]]) {
            SchemaBuilder.builder().map().values().`type`(inferSchema(t.typeArgs(1)))
          } else if (t <:< typeOf[Iterable[_]]) {
            SchemaBuilder.builder().array().items().`type`(inferSchema(t.typeArgs(0)))
          } else if (t <:< typeOf[scala.Enumeration#Value]) {
            t match {
              case TypeRef(enumType, _, _) =>
                val moduleMirror = rootMirror.reflectModule(enumType.termSymbol.asModule)
                val instanceMirror = rootMirror.reflect(moduleMirror.instance)
                val methodMirror = instanceMirror.reflectMethod(enumType.member(TermName("values")).asMethod)
                val enumSymbols = methodMirror().asInstanceOf[Enumeration#ValueSet]
                val args = enumSymbols.toSeq.map(_.toString)
                SchemaBuilder.builder().enumeration(enumType.toString.dropRight(5)).symbols(args: _*)
            }
          } else if (t <:< typeOf[AvroRecord[_]]) {

            val moduleMirror = rootMirror.reflectModule(t.typeSymbol.companion.asModule)
            val companionMirror = rootMirror.reflect(moduleMirror.instance)
            val constructor = t.decl(universe.termNames.CONSTRUCTOR)
            val params = constructor.asMethod.paramLists(0)
            val assembler = params.zipWithIndex.foldLeft(SchemaBuilder.record(t.toString).fields()) {
              case (assembler, (symbol, i)) =>
                val defaultDef = companionMirror.symbol.typeSignature.member(TermName(s"apply$$default$$${i + 1}"))
                val field = assembler.name(symbol.name.toString).`type`(inferSchema(symbol.typeSignature))
                if (defaultDef == NoSymbol) {
                  field.noDefault()
                } else {
                  val methodMirror = companionMirror.reflectMethod(defaultDef.asMethod)
                  val default = methodMirror()
                  if (symbol.typeSignature <:< typeOf[scala.Enumeration#Value]) {
                    field.withDefault(default.asInstanceOf[Enumeration#Value].toString)
                  } else if (t <:< typeOf[Map[_, _]]) {
                    field.withDefault(default.asInstanceOf[Map[String, _]].asJava)
                  } else if (symbol.typeSignature <:< typeOf[Iterable[_]]) {
                    field.withDefault(default.asInstanceOf[Iterable[_]].toList.asJava)
                  } else {
                    field.withDefault(default)
                  }
                }
            }
            assembler.endRecord()
          } else {
            //TODO maybe Avro Case Class support for UUID as fixed 16 bytes or 2x long
            throw new IllegalArgumentException("Unsupported Avro Case Class type " + t.toString)
          }

        typeSchemaCache.put(t, schema)
        schema
    }
  }
}

abstract class AvroRecord[X: TypeTag] extends SpecificRecord with java.io.Serializable {

  @JsonIgnore val schema: Schema = AvroRecord.inferSchema(typeOf[X])
  private val schemaFields = schema.getFields
  private val params = getClass.getConstructors()(0).getParameters
  require(params.length == schemaFields.size,
    s"number of constructor arguments (${params.length}) is not equal to schema field count (${schemaFields.size})")

  @transient private val declaredFields = getClass.getDeclaredFields
  @transient private val fields = params.zipWithIndex.map { case (param, pos) => {
    val field = declaredFields(pos)
    require(param.getType == field.getType,
      s"field `${field.getType}` at position $pos doesn't match expected `$param`")
    field.setAccessible(true)
    pos -> field
  }
  }.toMap

  override def getSchema: Schema = schema

  final override def get(i: Int): AnyRef = {
    val schemaField = schema.getFields.get(i)
    val field = fields(i)
    schemaField.schema().getType match {
      case ARRAY => field.get(this).asInstanceOf[Iterable[_]].asJava
      case ENUM => new EnumSymbol(schemaField.schema, field.get(this))
      case MAP => field.get(this).asInstanceOf[Map[String, _]].asJava
      case _ => field.get(this)
    }
  }

  final override def put(i: Int, v: scala.Any): Unit = {
    throw new AvroRuntimeException("Scala AvroRecord is immutable")
  }
}
