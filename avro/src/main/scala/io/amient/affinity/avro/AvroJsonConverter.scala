package io.amient.affinity.avro

import java.io.{StringWriter, Writer}

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import org.codehaus.jackson.JsonFactory

import scala.collection.JavaConversions._

object AvroJsonConverter {


  //TODO def toAvro[T: TypeTag](data: Json, registry: AvroSchemaProvider)

  //TODO def toAvro(data: Json, schema: Schema): GenericRecord

  private val jfactory = new JsonFactory()

  def toJson(data: Any): String = {
    val out = new StringWriter()
    toJson(out, data)
    out.toString()
  }

  def toJson(writer: Writer, data: Any): Unit = toJson(writer, AvroRecord.inferSchema(data), data)

  def toJson(out: Writer, schema: Schema, data: Any): Unit = {
    val gen = jfactory.createJsonGenerator(out)

    def generate(datum: Any, schemas: List[Schema]): Unit = {
      def typeIsAllowed(t: Schema.Type) = schemas.exists(_.getType == t)

      schemas.map(_.getType) match {
        case Schema.Type.ENUM :: Nil => gen.writeString(datum.toString)
        case Schema.Type.UNION :: Nil => generate(datum, schemas(0).getTypes.toList)
        case _ => datum match {
          case null if typeIsAllowed(Schema.Type.NULL) => gen.writeNull()
          case null => throw new IllegalArgumentException("Illegal null value for schemas: " + schemas.map(_.getType).mkString(","))
          case b: Boolean if typeIsAllowed(Schema.Type.BOOLEAN) => gen.writeBoolean(b)
          case b: Byte if typeIsAllowed(Schema.Type.INT) => gen.writeNumber(b)
          case i: Int if typeIsAllowed(Schema.Type.INT) => gen.writeNumber(i)
          case l: Long if typeIsAllowed(Schema.Type.LONG) => gen.writeNumber(l)
          case f: Float if typeIsAllowed(Schema.Type.FLOAT) => gen.writeNumber(f)
          case d: Double if typeIsAllowed(Schema.Type.DOUBLE) => gen.writeNumber(d)
          case s: String if typeIsAllowed(Schema.Type.STRING) => gen.writeString(s)
          case s: Utf8 if typeIsAllowed(Schema.Type.STRING) => gen.writeString(s.toString)
          case b: java.nio.ByteBuffer if typeIsAllowed(Schema.Type.BYTES) => gen.writeBinary(b.array())
          case b: Array[Byte] if typeIsAllowed(Schema.Type.FIXED) => gen.writeBinary(b)
          case r: IndexedRecord if typeIsAllowed(Schema.Type.RECORD) =>
            gen.writeStartObject()
            schemas(0).getFields.zipWithIndex.foreach { case (f, i) =>
              gen.writeFieldName(f.name())
              generate(r.get(i), List(f.schema()))
            }
            gen.writeEndObject()
          case i: Seq[_] if typeIsAllowed(Schema.Type.ARRAY) =>
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
            gen.writeStartArray()
            i.foreach(generate(_, s))
            gen.writeEndArray()
          case i: Iterable[_] if typeIsAllowed(Schema.Type.ARRAY) =>
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
            gen.writeStartArray()
            i.toSeq.foreach(generate(_, s))
            gen.writeEndArray()
          case w: java.util.AbstractCollection[_] if schemas.exists(_.getType == Schema.Type.ARRAY) =>
            gen.writeStartArray()
            w.toSeq.foreach(generate(_, schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)))
            gen.writeEndArray()
          case i: Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
            gen.writeStartObject()
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getValueType)
            i.toSeq.foreach { case (k, v) =>
              gen.writeFieldName(k.toString)
              generate(v, s)
            }
            gen.writeEndObject()
          case i: java.util.Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
            gen.writeStartObject()
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getValueType)
            i.toSeq.foreach { case (k, v) =>
              gen.writeFieldName(k.toString)
              generate(v, s)
            }
            gen.writeEndObject()
          case x => throw new IllegalArgumentException(s"Unsupported avro-json conversion for ${x.getClass} and allowed schemas ${schemas.mkString(",")}")
        }
      }
    }

    generate(data, List(schema))
    gen.flush()
  }
}
