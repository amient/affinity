package io.amient.affinity.avro

import io.amient.affinity.avro.record.AvroRecord
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

object Status extends Enumeration {
  type Status = Value
  val OK, FAILED = Value
}

case class Referenced(A: Status.Value, B: Status.Value) extends AvroRecord

class AvroSchemaSpec extends FlatSpec with Matchers {

  "AvroRecord" should "not fail when referencing the same type in a single schema" in {
    val schemaJson = AvroRecord.inferSchema[Referenced].toString(true)
    new Schema.Parser().parse(schemaJson)
  }

}
