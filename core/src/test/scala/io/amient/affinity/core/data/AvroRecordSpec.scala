package io.amient.affinity.core.data

import io.amient.affinity.core.data.avro.AvroRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}

object Side extends Enumeration {
  type Side = Value
  val LEFT, RIGHT = Value
  val symbols= values.toList.map(_.toString)
  val schema: Schema = SchemaBuilder.enumeration("Side").namespace(getClass.getPackage.getName)
    .symbols(symbols:_*)
}

object Base {
  val schema = SchemaBuilder
    .record("Base").namespace(getClass.getPackage.getName)
    .fields()
    .name("id").`type`().intType().noDefault()
    .name("side").`type`(Side.schema).noDefault()
    .endRecord()

}

case class Base(val id: Int, side: Side.Value) extends AvroRecord(Base.schema)

object Composite {
  val schema = SchemaBuilder
    .record("Composite").namespace(getClass.getPackage.getName)
    .fields()
    .name("items").`type`().array().items().`type`(Base.schema).noDefault()
    .name("index").`type`().map().values().`type`(Base.schema).noDefault()
    .endRecord()
}
case class Composite(val items: Seq[Base], index: Map[String, Base]) extends AvroRecord(Composite.schema)


class AvroRecordSpec extends FlatSpec with Matchers {

  import Side._
  "write-read" should "produce the same record" in {
    val b1 = Base(1, LEFT)
    val b2 = Base(2, LEFT)
    val b3 = Base(3, RIGHT)
    val c = Composite(
      Seq(b1, b2, b3),
      Map("b1" -> b1, "b2" -> b2, "b3" -> b3)
    )
    val bytes = AvroRecord.write(c, Composite.schema)
    val cc = AvroRecord.read(bytes, classOf[Composite], Composite.schema)
    assert(c == cc)

  }

}
