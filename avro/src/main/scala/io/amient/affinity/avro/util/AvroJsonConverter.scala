package io.amient.affinity.avro.util

import java.util

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import scala.collection.JavaConversions._

//TODO #70 instead of circe use simple jackson
//object AvroJsonConverter {
//
//  //TODO def toAvro[T: TypeTag](data: Json, registry: AvroSchemaProvider)
//
//  //TODO def toAvro(data: Json, schema: Schema): GenericRecord
//
//  def toJsonString(data: IndexedRecord): String = toJson(data, data.getSchema).toString()
//
//  def toJsonString(data: Any, schema: Schema): String = toJson(data, schema).toString()
//
//  def toJson(data: IndexedRecord): Json = toJson(data, List(data.getSchema))
//
//  def toJson(data: Any, schema: Schema): Json = toJson(data, List(schema))
//
//  private def toJson(datum: Any, schemas: List[Schema]): Json = {
//    def typeIsAllowed(t: Schema.Type) = schemas.exists(_.getType == t)
//    schemas.map(_.getType) match {
//      case Schema.Type.ENUM :: Nil => Json.fromString(datum.toString)
//      case Schema.Type.UNION :: Nil => toJson(datum, schemas(0).getTypes.toList)
//      case Schema.Type.RECORD :: Nil =>
//        val record = datum.asInstanceOf[IndexedRecord]
//        val f = schemas(0).getFields.zipWithIndex.map { case (f, i) =>
//          (f.name(), toJson(record.get(i), List(f.schema())))
//        }.toList
//        Json.fromFields(f)
//      //TODO case Schema.Type.FIXED :: Nil =>
//      //TODO case Schema.Type.BYTES :: Nil =>
//      case _ => datum match {
//        case null if typeIsAllowed(Schema.Type.NULL) => Json.Null
//        case null => throw new IllegalArgumentException("Illegal null value for schemas: " + schemas.map(_.getType).mkString(","))
//        case b: Boolean if typeIsAllowed(Schema.Type.BOOLEAN) => Json.fromBoolean(b)
//        case b: Byte if typeIsAllowed(Schema.Type.INT) => Json.fromInt(b)
//        case i: Int if typeIsAllowed(Schema.Type.INT) => Json.fromInt(i)
//        case l: Long if typeIsAllowed(Schema.Type.LONG) => Json.fromLong(l)
//        case f: Float if typeIsAllowed(Schema.Type.FLOAT) => Json.fromDoubleOrNull(f.toDouble)
//        case d: Double if typeIsAllowed(Schema.Type.DOUBLE) => Json.fromDoubleOrNull(d)
//        case s: String if typeIsAllowed(Schema.Type.STRING) => Json.fromString(s)
//        case i: Seq[_] if typeIsAllowed(Schema.Type.ARRAY) =>
//          val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
//          Json.arr(i.map(toJson(_, s)): _*)
//        case i: Iterable[_] if typeIsAllowed(Schema.Type.ARRAY) =>
//          val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
//          Json.arr(i.toSeq.map(toJson(_, s)): _*)
//        case w: util.AbstractCollection[_] if schemas.exists(_.getType == Schema.Type.ARRAY) => Json.arr(w.toSeq.map(toJson(_, schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType))): _*)
//        case i: Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
//          val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getValueType)
//          Json.fromFields(i.toSeq.map { case (k, v) => (k.toString, toJson(v, s)) })
//        case i: java.util.Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
//          val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getValueType)
//          Json.fromFields(i.toSeq.map { case (k, v) => (k.toString, toJson(v, s)) })
//        case x => throw new IllegalArgumentException(s"Unsupported avro-json conversion for ${x.getClass}")
//      }
//    }
//  }
//}
