package io.amient.affinity.avro

import java.util.UUID

import io.amient.affinity.avro.record.{AvroJsonConverter, AvroRecord, Union}
import org.scalatest.{FlatSpec, Matchers}

sealed trait Pet {
  def name: String
}

@Union(1)
case class Cat(name: String, opt: Option[Int] = Some(1)) extends Pet

@Union(2)
case class Dog(name: String) extends Pet

case class Me(myPet: Pet) extends AvroRecord

case class They(theirPets: List[Pet]) extends AvroRecord


sealed abstract class AbstractPet(chipit: Option[UUID]) {
  override def toString() = chipit.toString
}

@Union(1)
case class ConcreteDog(name: String, bark: Boolean, chipit: UUID) extends AbstractPet(Some(chipit))

@Union(2)
case class ConcreteCat(name: String, meow: Boolean, chipit: UUID) extends AbstractPet(Some(chipit))

@Union(3)
case class ConcreteRat() extends AbstractPet(None)

case class AbstractMe(myPet: AbstractPet) extends AvroRecord

case class AbstractThey(theirPets: List[AbstractPet]) extends AvroRecord


class AvroUnionSpec extends FlatSpec with Matchers {

  "AvroRecord" should "use old fields when Alias annotation is used" in {
    val schema = AvroRecord.inferSchema[AliasedAvro]
    schema.toString should be("{\"type\":\"record\",\"name\":\"AliasedAvro\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"Really cool string that was once renamed!\",\"aliases\":[\"old_name1\",\"old_name2\"]}]}")
  }

  behavior of "sealed traits in avro conversion"

  final val me1 = Me(Dog("Finn"))
  final val schemaMe = AvroRecord.inferSchema[Me]

  final val they1 = They(List(Dog("Finn"), Cat("Fionna")))
  final val schemaThey = AvroRecord.inferSchema[They]

  it should "infer schema of union of all concrete types of a sealed trait" in {
    schemaMe.toString should be("{\"type\":\"record\",\"name\":\"Me\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"myPet\",\"type\":[{\"type\":\"record\",\"name\":\"Cat\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"opt\",\"type\":[\"int\",\"null\"],\"default\":1}]},{\"type\":\"record\",\"name\":\"Dog\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}]}]}")
    schemaThey.toString should be("{\"type\":\"record\",\"name\":\"They\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"theirPets\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Cat\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"opt\",\"type\":[\"int\",\"null\"],\"default\":1}]},{\"type\":\"record\",\"name\":\"Dog\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}]}}]}")
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

  behavior of "sealed abstract class in avro conversion"


  it should "infer schema of union of all concrete types of a sealed abstract class" in {
    AvroRecord.inferSchema[AbstractMe].toString(true) should be("""|{
                                                                   |  "type" : "record",
                                                                   |  "name" : "AbstractMe",
                                                                   |  "namespace" : "io.amient.affinity.avro",
                                                                   |  "fields" : [ {
                                                                   |    "name" : "myPet",
                                                                   |    "type" : [ {
                                                                   |      "type" : "record",
                                                                   |      "name" : "ConcreteDog",
                                                                   |      "fields" : [ {
                                                                   |        "name" : "name",
                                                                   |        "type" : "string"
                                                                   |      }, {
                                                                   |        "name" : "bark",
                                                                   |        "type" : "boolean"
                                                                   |      }, {
                                                                   |        "name" : "chipit",
                                                                   |        "type" : {
                                                                   |          "type" : "fixed",
                                                                   |          "name" : "chipit",
                                                                   |          "namespace" : "",
                                                                   |          "size" : 16,
                                                                   |          "logicalType" : "uuid"
                                                                   |        }
                                                                   |      } ]
                                                                   |    }, {
                                                                   |      "type" : "record",
                                                                   |      "name" : "ConcreteCat",
                                                                   |      "fields" : [ {
                                                                   |        "name" : "name",
                                                                   |        "type" : "string"
                                                                   |      }, {
                                                                   |        "name" : "meow",
                                                                   |        "type" : "boolean"
                                                                   |      }, {
                                                                   |        "name" : "chipit",
                                                                   |        "type" : "chipit"
                                                                   |      } ]
                                                                   |    }, {
                                                                   |      "type" : "record",
                                                                   |      "name" : "ConcreteRat",
                                                                   |      "fields" : [ ]
                                                                   |    } ]
                                                                   |  } ]
                                                                   |}""".stripMargin)
  }

  it should "serialize sealed abstract class extensions as union members" in {
    val schema = AvroRecord.inferSchema[AbstractMe]
    val me: AbstractMe = AbstractMe(ConcreteDog("kitty", true, UUID.fromString("01010101-0202-0202-0303-030304040404")))
    val bytes = AvroRecord.write(me, schema)
    val m2: AbstractMe = AvroRecord.read[AbstractMe](bytes, schema)
    m2 should be(me)
  }

  it should "json marshall sealed abstract class whose extensions contain logical types" in {
    val me: AbstractMe = AbstractMe(ConcreteDog("finny", true, UUID.fromString("01010101-0202-0202-0303-030304040404")))
    val json = AvroJsonConverter.toJson(me, true)
    json should be(
      """{
        |  "myPet" : {
        |    "io.amient.affinity.avro.ConcreteDog" : {
        |      "name" : "finny",
        |      "bark" : true,
        |      "chipit" : "01010101-0202-0202-0303-030304040404"
        |    }
        |  }
        |}""".stripMargin)
  }

}
