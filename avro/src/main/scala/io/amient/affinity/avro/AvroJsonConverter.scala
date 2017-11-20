package io.amient.affinity.avro

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import scala.collection.JavaConversions._

object AvroJsonConverter {

  //TODO def toAvro[T: TypeTag](data: Json, registry: AvroSchemaProvider)

  //TODO def toAvro(data: Json, schema: Schema): GenericRecord

  private val jfactory = new JsonFactory()

  def toJson(data: IndexedRecord): String = toJson(data, data.getSchema)

  def toJson(data: Any, schema: Schema): String = {
    val stream = new ByteArrayOutputStream()
    val j = jfactory.createGenerator(stream, JsonEncoding.UTF8)

    def generate(datum: Any, schemas: List[Schema]): Unit = {
      def typeIsAllowed(t: Schema.Type) = schemas.exists(_.getType == t)
      schemas.map(_.getType) match {
        case Schema.Type.ENUM :: Nil => j.writeString(datum.toString)
        case Schema.Type.UNION :: Nil => generate(datum, schemas(0).getTypes.toList)
        case Schema.Type.RECORD :: Nil =>
          val record = datum.asInstanceOf[IndexedRecord]
          j.writeStartObject()
          schemas(0).getFields.zipWithIndex.foreach { case (f, i) =>
            j.writeFieldName(f.name())
            generate(record.get(i), List(f.schema()))
          }
          j.writeEndObject()
        case Schema.Type.BYTES :: Nil => j.writeBinary(datum.asInstanceOf[java.nio.ByteBuffer].array())
        case Schema.Type.FIXED :: Nil => j.writeBinary(datum.asInstanceOf[Array[Byte]])
        case _ => datum match {
          case null if typeIsAllowed(Schema.Type.NULL) => j.writeNull()
          case null => throw new IllegalArgumentException("Illegal null value for schemas: " + schemas.map(_.getType).mkString(","))
          case b: Boolean if typeIsAllowed(Schema.Type.BOOLEAN) => j.writeBoolean(b)
          case b: Byte if typeIsAllowed(Schema.Type.INT) => j.writeNumber(b)
          case i: Int if typeIsAllowed(Schema.Type.INT) => j.writeNumber(i)
          case l: Long if typeIsAllowed(Schema.Type.LONG) => j.writeNumber(l)
          case f: Float if typeIsAllowed(Schema.Type.FLOAT) => j.writeNumber(f)
          case d: Double if typeIsAllowed(Schema.Type.DOUBLE) => j.writeNumber(d)
          case s: String if typeIsAllowed(Schema.Type.STRING) => j.writeString(s)
          case i: Seq[_] if typeIsAllowed(Schema.Type.ARRAY) =>
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
            j.writeStartArray()
            i.foreach(generate(_, s))
            j.writeEndArray()
          case i: Iterable[_] if typeIsAllowed(Schema.Type.ARRAY) =>
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
            j.writeStartArray()
            i.toSeq.foreach(generate(_, s))
            j.writeEndArray()
          case w: java.util.AbstractCollection[_] if schemas.exists(_.getType == Schema.Type.ARRAY) =>
            j.writeStartArray()
            w.toSeq.foreach(generate(_, schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)))
            j.writeEndArray()
          case i: Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
            j.writeStartObject()
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getValueType)
            i.toSeq.foreach { case (k, v) =>
              j.writeFieldName(k.toString)
              generate(v, s)
            }
            j.writeEndObject()
          case i: java.util.Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
            j.writeStartObject()
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getValueType)
            i.toSeq.foreach { case (k, v) =>
              j.writeFieldName(k.toString)
              generate(v, s)
            }
            j.writeEndObject()
          case x => throw new IllegalArgumentException(s"Unsupported avro-json conversion for ${x.getClass}")
        }
      }
    }
    generate(data, List(schema))
    stream.toString()
  }
}
