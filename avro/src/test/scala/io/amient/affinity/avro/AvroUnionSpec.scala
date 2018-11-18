package io.amient.affinity.avro

import io.amient.affinity.avro.record.{AvroJsonConverter, AvroRecord}
import org.scalatest.{FlatSpec, Matchers}

sealed trait Pet {
  def name: String
}

case class Cat(name: String, opt: Option[Int] = Some(1)) extends Pet

case class Dog(name: String) extends Pet

case class Me(myPet: Pet) extends AvroRecord

case class They(theirPets: List[Pet]) extends AvroRecord

class AvroUnionSpec extends FlatSpec with Matchers {

  "AvroRecord" should "use old fields when Alias annotation is used" in {
    val schema = AvroRecord.inferSchema[AliasedAvro]
    schema.toString should be("{\"type\":\"record\",\"name\":\"AliasedAvro\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"description\":\"Really cool string that was once renamed!\"},\"aliases\":[\"old_name1\",\"old_name2\"]}]}")
  }

  behavior of "sealed traits in avro conversion"

  final val me1 = Me(Dog("Finn"))
  final val schemaMe = AvroRecord.inferSchema[Me]

  final val they1 = They(List(Dog("Finn"), Cat("Fionna")))
  final val schemaThey = AvroRecord.inferSchema[They]

  it should "infer schema of union of all concrete types of a sealed trait" in {
    schemaMe.toString should be ("{\"type\":\"record\",\"name\":\"Me\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"myPet\",\"type\":[{\"type\":\"record\",\"name\":\"Cat\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"opt\",\"type\":[\"int\",\"null\"],\"default\":1}]},{\"type\":\"record\",\"name\":\"Dog\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}]}]}")
    schemaThey.toString should be ("{\"type\":\"record\",\"name\":\"They\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"theirPets\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Cat\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"opt\",\"type\":[\"int\",\"null\"],\"default\":1}]},{\"type\":\"record\",\"name\":\"Dog\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}]}}]}")
  }

  it should "serialize and deserialize to and from binary" in {
    val bytes = AvroRecord.write(me1, schemaMe)
    val recoded = AvroRecord.read[Me](bytes, schemaMe)
    recoded should equal(me1)
    AvroRecord.read[Me](AvroRecord.write(they1, schemaThey), schemaThey) should equal(they1)
  }

  it should "serialize to json a correct type" in {
    val json1 = AvroJsonConverter.toJson(me1)
    json1 should be("{\"myPet\":{\"io.amient.affinity.avro.Dog\":{\"name\":\"Finn\"}}}")
    AvroJsonConverter.toAvro(json1, schemaMe) should equal(me1)
    val json2 = AvroJsonConverter.toJson(they1)
    json2 should be("{\"theirPets\":[{\"io.amient.affinity.avro.Dog\":{\"name\":\"Finn\"}},{\"io.amient.affinity.avro.Cat\":{\"name\":\"Fionna\",\"opt\":1}}]}")
    AvroJsonConverter.toAvro(json2, schemaThey) should equal(they1)
  }

}
