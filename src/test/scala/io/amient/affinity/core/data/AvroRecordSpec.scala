package io.amient.affinity.core.data

import org.apache.avro.SchemaBuilder
import org.scalatest.{FlatSpec, Matchers}

object Base {
  val schema = SchemaBuilder
    .record("Base").namespace(getClass.getPackage.getName)
    .fields()
    .name("id").`type`().intType().noDefault().endRecord()
}
case class Base(val id: Int) extends AvroRecord(Base.schema)

object Composite {
  val schema = SchemaBuilder
    .record("Composite").namespace(getClass.getPackage.getName)
    .fields()
    .name("items").`type`().array().items().`type`(Base.schema).noDefault().endRecord()
}
case class Composite(val items: Seq[Base]) extends AvroRecord(Composite.schema)


class AvroRecordSpec extends FlatSpec with Matchers {

  "" should "" in {
    val c = Composite(Seq(Base(1), Base(2), Base(3)))
    println(c)
    val bytes = AvroRecord.write(c, Composite.schema)
    val cc = AvroRecord.read(bytes, classOf[Composite], Composite.schema)
    println(cc)
    assert(c == cc)

  }



}
