package io.amient.affinity.avro

import io.amient.affinity.avro.SimpleEnum.SimpleEnum
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe._

class AvroJsonConverterSpec extends FlatSpec with Matchers {

  behavior of "AvroJsonConverter"

  it should "serialize to and from case class <-> avro <-> json with identical result to circe lib" in {
    val msg = AvroNamedRecords(SimpleKey(99), Some(SimpleKey(99)), None, List(SimpleKey(99), SimpleKey(100)), List(None, Some(SimpleKey(99)), None))
    val avroJson = AvroJsonConverter.toJson(msg)

    avroJson should be ("{\"e\":{\"id\":99},\"rn\":{\"id\":99},\"rs\":null,\"l\":[{\"id\":99},{\"id\":100}],\"lo\":[null,{\"id\":99},null]}")

    AvroJsonConverter.toAvro(avroJson, msg.getSchema()) should be (msg)

    val msg2 = AvroEnums(SimpleEnum.B, Some(SimpleEnum.B), None, List(SimpleEnum.A, SimpleEnum.B), List(None, Some(SimpleEnum.B)))
    val avroJson2 = AvroJsonConverter.toJson(msg2)
    avroJson2 should be("{\"raw\":\"B\",\"on\":\"B\",\"sd\":null,\"l\":[\"A\",\"B\"],\"lo\":[null,\"B\"]}")
    AvroJsonConverter.toAvro(avroJson2, msg2.getSchema()) should be (msg2)

  }

}
