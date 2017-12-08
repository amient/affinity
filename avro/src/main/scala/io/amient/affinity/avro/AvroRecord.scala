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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import io.amient.affinity.avro.schema.AvroSchemaProvider
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic._
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.{ByteBufferInputStream, Utf8}
import org.apache.avro.{AvroRuntimeException, Schema, SchemaBuilder}
import org.codehaus.jackson.annotate.JsonIgnore

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object AvroRecord {

  private val MAGIC: Byte = 0

  private val typeSchemaCache = new ConcurrentHashMap[Type, Schema]()

  val INT_SCHEMA = Schema.create(Schema.Type.INT)
  val BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN)
  val LONG_SCHEMA = Schema.create(Schema.Type.LONG)
  val FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT)
  val DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE)
  val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
  val BYTES_SCHEMA = Schema.create(Schema.Type.BYTES)
  val NULL_SCHEMA = Schema.create(Schema.Type.NULL)


  def write(x: IndexedRecord, schemaId: Int): Array[Byte] = {
    write(x, x.getSchema, schemaId)
  }

  def write(x: Any, schema: Schema, schemaId: Int = -1): Array[Byte] = {
    if (x == null) {
      return null
    }
    val valueOut = new ByteArrayOutputStream()
    try {
      val encoder = EncoderFactory.get().binaryEncoder(valueOut, null)
      val writer = new GenericDatumWriter[Any](schema)
      if (schemaId >= 0) {
        valueOut.write(MAGIC)
        ByteUtils.writeIntValue(schemaId, valueOut)
      }
      writer.write(x, encoder)
      encoder.flush()
      valueOut.toByteArray
    } finally {
      valueOut.close
    }
  }

  def read[T: TypeTag](bytes: Array[Byte], cls: Class[T], schema: Schema): T = read(bytes, cls, schema, schema)

  def read[T: TypeTag](record: GenericContainer): T = {
    readDatum(record, typeOf[T], record.getSchema).asInstanceOf[T]
  }

  def read[T: TypeTag](bytes: Array[Byte], cls: Class[T], writerSchema: Schema, readerSchema: Schema): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new GenericDatumReader[GenericRecord](writerSchema, readerSchema)
    val record: GenericRecord = reader.read(null, decoder)
    read(record)
  }

  private def deriveValue(schemaField: Schema, value: Any): AnyRef = {
    schemaField.getType match {
      case ARRAY => value.asInstanceOf[Iterable[_]].map(deriveValue(schemaField.getElementType, _)).asJava
      case ENUM => new EnumSymbol(schemaField, value)
      case MAP => value.asInstanceOf[Map[String, _]].mapValues(deriveValue(schemaField.getValueType, _)).asJava
      case UNION => value match {
        case None => null
        case Some(x) => deriveValue(schemaField.getTypes.get(1), x)
      }
      case _ => value match {
        case ref: AnyRef => ref
        case any => throw new NotImplementedError(s"Unsupported avro type conversion for $any")
      }
    }
  }


  /**
    *
    * @param bytes
    * @param schemaRegistry
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytes: Array[Byte], schemaRegistry: AvroSchemaProvider): Any = {
    if (bytes == null) null else read(new ByteArrayInputStream(bytes), schemaRegistry)
  }

  /**
    *
    * @param bytes ByteBuffer version of the registered avro reader
    * @param schemaRegistry
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytes: ByteBuffer, schemaRegistry: AvroSchemaProvider): Any = {
    if (bytes == null) null else read(new ByteBufferInputStream(List(bytes).asJava), schemaRegistry)
  }

  /**
    *
    * @param bytesIn InputStream implementation for the registered avro reader
    * @param schemaRegistry
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytesIn: InputStream, schemaRegistry: AvroSchemaProvider): Any = {
    require(bytesIn.read() == MAGIC)
    val schemaId = ByteUtils.readIntValue(bytesIn)
    require(schemaId >= 0)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytesIn, null)
    schemaRegistry.schema(schemaId) match {
      case None => throw new IllegalArgumentException(s"Schema $schemaId doesn't exist")
      case Some(writerSchema) =>
        schemaRegistry.getCurrentSchema(writerSchema.getFullName) match {
          case None =>
            //case classes are not present in this runtime, just use GenericRecord
            val reader = new GenericDatumReader[GenericRecord](writerSchema, writerSchema)
            reader.read(null, decoder)
          case Some((_, readerSchema)) =>
            //http://avro.apache.org/docs/1.7.2/api/java/org/apache/avro/io/parsing/doc-files/parsing.html
            val reader = new GenericDatumReader[Any](writerSchema, readerSchema)
            val record = reader.read(null, decoder)
            read(record, readerSchema)
        }
    }
  }

  def read(record: Any, schema: Schema) = {
    val tpe = schema.getType match {
      case NULL => typeOf[Null]
      case BOOLEAN => typeOf[Boolean]
      case INT => typeOf[Int]
      case LONG => typeOf[Long]
      case FLOAT => typeOf[Float]
      case DOUBLE => typeOf[Double]
      case STRING => typeOf[String]
      case BYTES => typeOf[java.nio.ByteBuffer]
      case _ =>
        val cls = Class.forName(schema.getFullName)
        val m = runtimeMirror(cls.getClassLoader)
        m.staticClass(cls.getName).selfType
    }
    readDatum(record, tpe , schema)
  }

  def readDatum(datum: Any, tpe: Type, schema: Schema): Any = {
    schema.getType match {
      case BOOLEAN => new java.lang.Boolean(datum.asInstanceOf[Boolean])
      case INT => new java.lang.Integer(datum.asInstanceOf[Int])
      case NULL => null
      case FLOAT => new java.lang.Float(datum.asInstanceOf[Float])
      case DOUBLE => new java.lang.Double(datum.asInstanceOf[Double])
      case LONG => new java.lang.Long(datum.asInstanceOf[Long])
      case BYTES => datum.asInstanceOf[java.nio.ByteBuffer]
      case STRING if datum == null => null
      case STRING => String.valueOf(datum.asInstanceOf[Utf8])
      case RECORD if datum == null => null
      case RECORD =>
        val record = datum.asInstanceOf[IndexedRecord]
        val constructor = tpe.decl(universe.termNames.CONSTRUCTOR).asMethod
        val params = constructor.paramLists(0)
        val arguments = record.getSchema.getFields.asScala.map { field =>
          readDatum(record.get(field.pos), params(field.pos).typeSignature, field.schema)
        }
        val typeMirror = universe.runtimeMirror(Class.forName(tpe.typeSymbol.asClass.fullName).getClassLoader)
        val classMirror = typeMirror.reflectClass(tpe.typeSymbol.asClass)
        val constructorMirror = classMirror.reflectConstructor(constructor)
        constructorMirror(arguments: _*)
      case ENUM =>
        tpe match {
        case TypeRef(enumType, _, _) =>
          val moduleMirror = rootMirror.reflectModule(enumType.termSymbol.asModule)
          val instanceMirror = rootMirror.reflect(moduleMirror.instance)
          val methodMirror = instanceMirror.reflectMethod(enumType.member(TermName("withName")).asMethod)
          methodMirror(datum.asInstanceOf[EnumSymbol].toString)
      }
      case MAP => datum.asInstanceOf[java.util.Map[Utf8, _]].asScala.toMap
        .map { case (k, v) => (
          k.toString,
          readDatum(v, tpe.typeArgs(1), schema.getValueType))
        }
      case ARRAY => val iterable = datum.asInstanceOf[java.util.Collection[_]].asScala
        .map(item => readDatum(item, tpe.typeArgs(0), schema.getElementType))
        if (tpe <:< typeOf[Set[_]]) {
          iterable.toSet
        } else if (tpe <:< typeOf[List[_]]) {
          iterable.toList
        } else if (tpe <:< typeOf[Vector[_]]) {
          iterable.toVector
        } else if (tpe <:< typeOf[IndexedSeq[_]]) {
          iterable.toIndexedSeq
        } else if (tpe <:< typeOf[Seq[_]]) {
          iterable.toSeq
        } else {
          iterable
        }
      case UNION if (tpe <:< typeOf[Option[_]]) =>
        datum match {
          case null => None
          case some => Some(readDatum(some, tpe.typeArgs(0), schema.getTypes.get(1)))
        }
      case UNION => throw new NotImplementedError("Only Option-like Avro Unions are supported, e.g. union(null, X)")
      case FIXED => throw new NotImplementedError("Avro Fixed are not supported")
    }
  }

  def inferSchema(fqn: String): Schema = {
    fqn match {
      case "null" => NULL_SCHEMA
      case "boolean" => BOOLEAN_SCHEMA
      case "int" => INT_SCHEMA
      case "long" => LONG_SCHEMA
      case "float" => FLOAT_SCHEMA
      case "double" => DOUBLE_SCHEMA
      case "string" => STRING_SCHEMA
      case _ =>
        val cls = Class.forName(fqn)
        val m = runtimeMirror(cls.getClassLoader)
        inferSchema(m.staticClass(cls.getName).selfType)
    }
  }

  def inferSchema(obj: Any): Schema = {
    obj match {
      case container: GenericContainer => container.getSchema
      case null => AvroRecord.NULL_SCHEMA
      case _: Boolean => AvroRecord.BOOLEAN_SCHEMA
      case _: Byte => AvroRecord.INT_SCHEMA
      case _: Int => AvroRecord.INT_SCHEMA
      case _: Long => AvroRecord.LONG_SCHEMA
      case _: Float => AvroRecord.FLOAT_SCHEMA
      case _: Double => AvroRecord.DOUBLE_SCHEMA
      case _: String => AvroRecord.STRING_SCHEMA
      case _: java.nio.ByteBuffer => AvroRecord.BYTES_SCHEMA
      case _ =>
        val m = runtimeMirror(obj.getClass.getClassLoader)
        val classSymbol = m.staticClass(obj.getClass.getName)
        AvroRecord.inferSchema(classSymbol.selfType)
    }
  }

  def inferSchema[X: TypeTag, AnyRef <: X](cls: Class[X]): Schema = inferSchema(typeOf[X])

  def inferSchema(tpe: Type): Schema = {
    typeSchemaCache.get(tpe) match {
      case some if some != null => some
      case _ =>
        val schema: Schema =
          if (tpe =:= definitions.IntTpe) {
            SchemaBuilder.builder().intType()
          } else if (tpe =:= definitions.LongTpe) {
            SchemaBuilder.builder().longType()
          } else if (tpe =:= definitions.BooleanTpe) {
            SchemaBuilder.builder().booleanType()
          } else if (tpe =:= definitions.FloatTpe) {
            SchemaBuilder.builder().floatType()
          } else if (tpe =:= definitions.DoubleTpe) {
            SchemaBuilder.builder().doubleType()
          } else if (tpe =:= typeOf[java.nio.ByteBuffer]) {
            SchemaBuilder.builder().bytesType()
          } else if (tpe =:= typeOf[String]) {
            SchemaBuilder.builder().stringType()
          } else if (tpe =:= typeOf[Null]) {
            SchemaBuilder.builder().nullType()
          } else if (tpe <:< typeOf[Map[String, _]]) {
            SchemaBuilder.builder().map().values().`type`(inferSchema(tpe.typeArgs(1)))
          } else if (tpe <:< typeOf[Iterable[_]]) {
            SchemaBuilder.builder().array().items().`type`(inferSchema(tpe.typeArgs(0)))
          } else if (tpe <:< typeOf[scala.Enumeration#Value]) {
            tpe match {
              case TypeRef(enumType, _, _) =>
                val typeMirror = universe.runtimeMirror(Class.forName(enumType.typeSymbol.asClass.fullName).getClassLoader)
                val moduleMirror = typeMirror.reflectModule(enumType.termSymbol.asModule)
                val instanceMirror = typeMirror.reflect(moduleMirror.instance)
                val methodMirror = instanceMirror.reflectMethod(enumType.member(TermName("values")).asMethod)
                val enumSymbols = methodMirror().asInstanceOf[Enumeration#ValueSet]
                val args = enumSymbols.toSeq.map(_.toString)
                SchemaBuilder.builder().enumeration(enumType.toString.dropRight(5)).symbols(args: _*)
            }
          } else if (tpe <:< typeOf[Option[_]]) {
            SchemaBuilder.builder().unionOf().nullType().and().`type`(inferSchema(tpe.typeArgs(0))).endUnion()
          } else if (tpe <:< typeOf[AvroRecord[_]]) {
            val typeMirror = universe.runtimeMirror(Class.forName(tpe.typeSymbol.asClass.fullName).getClassLoader)
            val moduleMirror = typeMirror.reflectModule(tpe.typeSymbol.companion.asModule)
            val companionMirror = typeMirror.reflect(moduleMirror.instance)
            val constructor = tpe.decl(universe.termNames.CONSTRUCTOR)
            val params = constructor.asMethod.paramLists(0)
            val assembler = params.zipWithIndex.foldLeft(SchemaBuilder.record(tpe.toString).fields()) {
              case (assembler, (symbol, i)) =>
                val fieldSchema = inferSchema(symbol.typeSignature)
                val field = assembler.name(symbol.name.toString).`type`(fieldSchema)
                val defaultDef = companionMirror.symbol.typeSignature.member(TermName(s"apply$$default$$${i + 1}"))
                if (defaultDef == NoSymbol) {
                  field.noDefault()
                } else {
                  val defaultGetter = companionMirror.reflectMethod(defaultDef.asMethod)
                  field.withDefault(deriveValue(fieldSchema, defaultGetter()))
                }
            }
            assembler.endRecord()
          } else {
            throw new IllegalArgumentException("Unsupported Avro Case Class type " + tpe.toString)
          }

        typeSchemaCache.put(tpe, schema)
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
  //TODO verify that when fields are not transient the java and kryo serialization is not excessive
  private val fields = params.zipWithIndex.map { case (param, pos) => {
    val field = declaredFields(pos)
    require(param.getType == field.getType,
      s"field `${field.getType}` at position $pos doesn't match expected `$param`")
    field.setAccessible(true)
    pos -> field
  }
  }.toMap

  override def getSchema: Schema = schema

  final override def get(i: Int): AnyRef = {
    AvroRecord.deriveValue(schema.getFields.get(i).schema(), fields(i).get(this))
  }

  final override def put(i: Int, v: scala.Any): Unit = {
    throw new AvroRuntimeException("Scala AvroRecord is immutable")
  }
}
