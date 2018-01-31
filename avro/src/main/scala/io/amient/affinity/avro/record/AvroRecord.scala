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

package io.amient.affinity.avro.record

import java.io.{ByteArrayOutputStream, OutputStream}
import java.lang.reflect.{Field, Parameter}
import java.util

import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import org.apache.avro.{AvroRuntimeException, Schema, SchemaBuilder}
import org.codehaus.jackson.annotate.JsonIgnore

import scala.annotation.StaticAnnotation
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

final class Alias(aliases: String*) extends StaticAnnotation

object AvroRecord extends AvroExtractors {

  val INT_SCHEMA = Schema.create(Schema.Type.INT)
  val BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN)
  val LONG_SCHEMA = Schema.create(Schema.Type.LONG)
  val FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT)
  val DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE)
  val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
  val BYTES_SCHEMA = Schema.create(Schema.Type.BYTES)
  val NULL_SCHEMA = Schema.create(Schema.Type.NULL)

  def write(value: Any, schema: Schema): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    try {
      write(value, schema, output)
      output.toByteArray
    } finally {
      output.close
    }
  }

  def write[O <: OutputStream](value: Any, schema: Schema, output: O): O = {
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    val writer = new GenericDatumWriter[Any](schema)
    writer.write(extract(value, List(schema)), encoder)
    encoder.flush()
    output
  }

  def read[T: TypeTag](bytes: Array[Byte], schema: Schema): T = read(bytes, schema, schema)

  def read[T: TypeTag](record: GenericContainer): T = {
    readDatum(record, typeOf[T], record.getSchema).asInstanceOf[T]
  }

  def read[T: TypeTag](bytes: Array[Byte], writerSchema: Schema, readerSchema: Schema): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new GenericDatumReader[Any](writerSchema, readerSchema)
    reader.read(null, decoder) match {
      case record: GenericRecord => read(record)
      case other => readDatum(other, typeOf[T], readerSchema).asInstanceOf[T]
    }
  }

  /**
    * deriveValue is used in reflection to get defaults for constructor arguments
    *
    * @param schemaField
    * @param value
    * @return
    */
  private def deriveValue(schemaField: Schema, value: Any): AnyRef = {
    schemaField.getType match {
      case BYTES => java.nio.ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      case ARRAY => value.asInstanceOf[Iterable[Any]].map(deriveValue(schemaField.getElementType, _)).asJava
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


  class LocalCache[K, I] extends ThreadLocal[java.util.HashMap[K, I]] {
    override def initialValue() = new util.HashMap[K, I]()

    def getOrInitialize(key: K, initializer: => I) = {
      get().get(key) match {
        case null =>
          val init = initializer
          get().put(key, init)
          init
        case some =>
          some
      }
    }
  }

  private object fqnMirrorCache extends LocalCache[String, universe.Mirror] {
    def getOrInitialize(fqn: String): universe.Mirror = {
      getOrInitialize(fqn, runtimeMirror(Class.forName(fqn).getClassLoader))
    }
  }

  private object fqnTypeCache extends LocalCache[String, Type] {
    def getOrInitialize(fqn: String): Type = {
      getOrInitialize(fqn, fqnMirrorCache.getOrInitialize(fqn).staticClass(fqn).selfType)
    }
  }

  private object fqnClassMirrorCache extends LocalCache[String, ClassMirror] {
    def getOrInitialize(fqn: String): ClassMirror = getOrInitialize(fqn, {
      val typeMirror = universe.runtimeMirror(Class.forName(fqn).getClassLoader)
      val tpe = fqnTypeCache.getOrInitialize(fqn)
      typeMirror.reflectClass(tpe.typeSymbol.asClass)
    })
  }

  private object classFieldsCache extends LocalCache[Class[_], Map[Int, Field]] {
    def getOrInitialize(cls: Class[_], schema: Schema): Map[Int, Field] = getOrInitialize(cls, {
      val schemaFields = schema.getFields
      val params: Array[Parameter] = cls.getConstructors()(0).getParameters
      require(params.length == schemaFields.size,
        s"number of constructor arguments (${params.length}) is not equal to schema field count (${schemaFields.size})")

      val declaredFields = cls.getDeclaredFields
      val fields: Map[Int, Field] = params.zipWithIndex.map { case (param, pos) => {
        val field = declaredFields(pos)
        require(param.getType == field.getType,
          s"field `${field.getType}` at position $pos doesn't match expected `$param`")
        field.setAccessible(true)
        pos -> field
      }
      }.toMap
      fields
    })
  }

  private object fqnConstructorCache extends LocalCache[String, (Seq[Type], MethodMirror)] {
    def getOrInitialize(fqn: String): (Seq[Type], MethodMirror) = getOrInitialize(fqn, {
      val tpe = fqnTypeCache.getOrInitialize(fqn)
      val constructor = tpe.decl(universe.termNames.CONSTRUCTOR).asMethod
      val params = constructor.paramLists(0).map(_.typeSignature)
      val classMirror = fqnClassMirrorCache.getOrInitialize(fqn)
      val constructorMirror: MethodMirror = classMirror.reflectConstructor(constructor)
      (params, constructorMirror)
    })
  }

  private object enumCache extends LocalCache[Type, MethodMirror] {
    def getOrInitialize(tpe: Type): MethodMirror = getOrInitialize(tpe, {
      tpe match {
        case TypeRef(enumType, _, _) =>
          val moduleMirror = rootMirror.reflectModule(enumType.termSymbol.asModule)
          val instanceMirror = rootMirror.reflect(moduleMirror.instance)
          instanceMirror.reflectMethod(enumType.member(TermName("withName")).asMethod)
      }
    })
  }

  private object iterableCache extends LocalCache[Type, (Iterable[Any]) => Iterable[Any]] {
    def getOrInitialize(tpe: Type): (Iterable[Any]) => Iterable[Any] = getOrInitialize(tpe, {
      if (tpe <:< typeOf[Set[_]]) {
        (iterable) => iterable.toSet
      } else if (tpe <:< typeOf[List[Any]]) {
        (iterable) => iterable.toList
      } else if (tpe <:< typeOf[Vector[Any]]) {
        (iterable) => iterable.toVector
      } else if (tpe <:< typeOf[IndexedSeq[Any]]) {
        (iterable) => iterable.toIndexedSeq
      } else if (tpe <:< typeOf[Seq[Any]]) {
        (iterable) => iterable.toSeq
      } else {
        (iterable) => iterable
      }
    })
  }

  private object unionCache extends LocalCache[Type, (Any, Schema) => Any] {
    def getOrInitialize(tpe: Type): (Any, Schema) => Any = getOrInitialize(tpe, {
      if (tpe <:< typeOf[Option[Any]]) {
        (datum, schema) =>
          datum match {
            case null => None
            case some => Some(readDatum(some, tpe.typeArgs(0), schema.getTypes.get(1)))
          }
      } else {
        (_, _) => throw new NotImplementedError(s"Only Option-like Avro Unions are supported, e.g. union(null, X), got: $tpe")
      }
    })
  }

  /**
    * Read avro value, e.g. GenericRecord or primitive into scala case class or scala primitive
    * @param record
    * @param schema
    * @return
    */
  def read(record: Any, schema: Schema): Any = {
    val tpe = schema.getType match {
      case NULL => typeOf[Null]
      case BOOLEAN => typeOf[Boolean]
      case INT => typeOf[Int]
      case LONG => typeOf[Long]
      case FLOAT => typeOf[Float]
      case DOUBLE => typeOf[Double]
      case STRING => typeOf[String]
      case BYTES => typeOf[Array[Byte]]
      case _ => fqnTypeCache.getOrInitialize(schema.getFullName)
    }
    readDatum(record, tpe, schema)
  }

  //TODO instead Any, this method should be readDatum[T: TypeTag](...):T because there are some unchecked .asInstanceOf[T]
  def readDatum[T: TypeTag](datum: Any, tpe: Type, schema: Schema): Any = {
    schema.getType match {
      case BOOLEAN => new java.lang.Boolean(datum.asInstanceOf[Boolean])
      case INT => new java.lang.Integer(datum.asInstanceOf[Int])
      case NULL => null
      case FLOAT => new java.lang.Float(datum.asInstanceOf[Float])
      case DOUBLE => new java.lang.Double(datum.asInstanceOf[Double])
      case LONG => new java.lang.Long(datum.asInstanceOf[Long])
      case BYTES => ByteUtils.bufToArray(datum.asInstanceOf[java.nio.ByteBuffer])
      case STRING if datum == null => null
      case STRING => String.valueOf(datum.asInstanceOf[Utf8])
      case ENUM => enumCache.getOrInitialize(tpe).apply(datum.asInstanceOf[EnumSymbol].toString)
      case UNION => unionCache.getOrInitialize(tpe)(datum, schema)
      case RECORD if datum == null => null
      case RECORD =>
        val record = datum.asInstanceOf[IndexedRecord]
        val (params: Seq[Type], constructorMirror) = fqnConstructorCache.getOrInitialize(record.getSchema.getFullName)
        val arguments = record.getSchema.getFields.asScala.map { field =>
          readDatum(record.get(field.pos), params(field.pos), field.schema)
        }
        constructorMirror(arguments: _*)
      case MAP => datum.asInstanceOf[java.util.Map[Utf8, _]].asScala.toMap
        .map { case (k, v) => (
          k.toString,
          readDatum(v, tpe.typeArgs(1), schema.getValueType))
        }
      case ARRAY =>
        iterableCache.getOrInitialize(tpe) {
          datum.asInstanceOf[java.util.Collection[Any]].asScala.map(
            item => readDatum(item, tpe.typeArgs(0), schema.getElementType))
        }
      case FIXED => throw new NotImplementedError("Avro Fixed are not supported")
    }
  }

  def inferSchema[T: TypeTag]: Schema = inferSchema(typeOf[T])

  def inferSchema(cls: Class[_]): Schema = {
    inferSchema(classTypeCache.getOrInitialize(cls, fqnTypeCache.getOrInitialize(cls.getName)))
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
      case _ => inferSchema(fqnTypeCache.getOrInitialize(fqn))
    }
  }

  def inferSchema(obj: Any): Schema = {
    //TODO use extractors here because the list is incomplete
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
      case _: Array[Byte] => AvroRecord.BYTES_SCHEMA
      case _ =>
        val m = runtimeMirror(obj.getClass.getClassLoader)
        val classSymbol = m.staticClass(obj.getClass.getName)
        AvroRecord.inferSchema(classSymbol.selfType)
    }
  }

  private object classTypeCache extends LocalCache[Class[_], Type]()

  private object typeSchemaCache extends LocalCache[Type, Schema]()

  def inferSchema(tpe: Type): Schema = {
    typeSchemaCache.getOrInitialize(tpe, {
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
      } else if (tpe =:= typeOf[Array[Byte]]) {
        SchemaBuilder.builder().bytesType()
      } else if (tpe =:= typeOf[String]) {
        SchemaBuilder.builder().stringType()
      } else if (tpe =:= typeOf[Null]) {
        SchemaBuilder.builder().nullType()
      } else if (tpe <:< typeOf[Map[String, Any]]) {
        SchemaBuilder.builder().map().values().`type`(inferSchema(tpe.typeArgs(1)))
      } else if (tpe <:< typeOf[Iterable[Any]]) {
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
      } else if (tpe <:< typeOf[Option[Any]]) {
        SchemaBuilder.builder().unionOf().nullType().and().`type`(inferSchema(tpe.typeArgs(0))).endUnion()
      } else if (tpe <:< typeOf[AvroRecord]) {
        val typeMirror = fqnMirrorCache.getOrInitialize(tpe.typeSymbol.asClass.fullName)
        val moduleMirror = typeMirror.reflectModule(tpe.typeSymbol.companion.asModule)
        val companionMirror = typeMirror.reflect(moduleMirror.instance)
        val constructor = tpe.decl(universe.termNames.CONSTRUCTOR)
        val params = constructor.asMethod.paramLists(0)
        val assembler = params.zipWithIndex.foldLeft(SchemaBuilder.record(tpe.toString).fields()) {
          case (assembler, (symbol, i)) =>
            val fieldSchema = inferSchema(symbol.typeSignature)
            val builder = assembler.name(symbol.name.toString)
            symbol.annotations.find(_.tree.tpe =:= typeOf[Alias]).foreach {
              a => builder.aliases(a.tree.children.tail.map(_.productElement(0).asInstanceOf[Constant].value.toString): _*)
            }
            val field = builder.`type`(fieldSchema)
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
    })
  }
}

abstract class AvroRecord extends SpecificRecord with java.io.Serializable {

  @JsonIgnore val schema: Schema = AvroRecord.inferSchema(getClass)

  @transient private[avro] var _serializedInstanceBytes: Array[Byte] = null

  private val fields: Map[Int, Field] = AvroRecord.classFieldsCache.getOrInitialize(getClass, schema)

  override def getSchema: Schema = schema

  final override def get(i: Int): AnyRef = {
    AvroRecord.deriveValue(schema.getFields.get(i).schema(), fields(i).get(this))
  }

  final override def put(i: Int, v: scala.Any): Unit = {
    throw new AvroRuntimeException("Scala AvroRecord is immutable")
  }
}
