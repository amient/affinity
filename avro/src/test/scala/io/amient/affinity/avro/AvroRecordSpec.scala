package io.amient.affinity.avro

import io.amient.affinity.avro.schema.MemorySchemaRegistry
import org.scalatest.{FlatSpec, Matchers}

class AvroRecordSpec extends FlatSpec with Matchers {

  /**
    * Data version 1 is written at some point in the past
    */
  val oldSerde = new MemorySchemaRegistry {
    register(classOf[Record_V1])

    //Future schema V2 available in old version - slightly unrealistic for embedded registry but here we're testing
    //the API for handling serialization, not the embedded implementation of registry, i.e. this would be true
    //in case of shared registry, like zookeeper of kafka-based, or confluent schema registry
    register(classOf[Record_V1], AvroRecord.inferSchema(classOf[Record]))
    register(classOf[SimpleRecord])
    initialize()

    override def close(): Unit = ()
  }

  /**
    * New version 2 of the case class and schema is added at the end of registry
    * the previous V1 schema version now points to the newest case class.
    */
  val newSerde = new AvroRecordTestSerde
  newSerde.initialize()

  "Data written with an older serde" should "be rendered into the current representation in a backward-compatible way" in {
    val oldValue = oldSerde.toBytes(Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), 10))
    val renderedValue = newSerde.fromBytes(oldValue)
    renderedValue should be(Record(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map()))
  }

  "Data Written with a newer serde" should "be rendered into the the current representation in a forward-compatible way" in {
    val newValue = newSerde.toBytes(Record(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("1" -> SimpleRecord(SimpleKey(1), SimpleEnum.A))))
    val downgradedValue = oldSerde.fromBytes(newValue)
    downgradedValue should be(Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), 0))
  }

  "Primitive types" should "be handled directly without registration" in {
    newSerde.fromBytes(newSerde.toBytes(true)) should equal(true)
    newSerde.fromBytes(newSerde.toBytes(100)) should equal(100)
    newSerde.fromBytes(newSerde.toBytes(100L)) should equal(100L)
    newSerde.fromBytes(newSerde.toBytes(1.0f)) should equal(1.0f)
    newSerde.fromBytes(newSerde.toBytes(10.01)) should equal(10.01)
    newSerde.fromBytes(newSerde.toBytes("hello")) should equal("hello")
  }

  "Scala Enums" should "be treated as EnumSymbols" in {
    val serialized = newSerde.toBytes(AvroEnums())
    newSerde.fromBytes(serialized) should be (AvroEnums())
    val a = AvroEnums(SimpleEnum.B, Some(SimpleEnum.B), None, List(SimpleEnum.A, SimpleEnum.B), List(None, Some(SimpleEnum.B)))
    val as = newSerde.toBytes(a)
    newSerde.fromBytes(as) should be (a)
  }

  "Optional types" should "be treated as union(null, X)" in {
    val emptySerialized = newSerde.toBytes(AvroPrmitives())
    val empty = newSerde.fromBytes(emptySerialized)
    empty should equal(AvroPrmitives())
    val nonEmpty = AvroPrmitives(
      None, Some(true),
      None, Some(Int.MinValue),
      None, Some(Long.MaxValue),
      None, Some(Float.MaxValue),
      None, Some(Double.MaxValue),
      None, Some("Hello")
    )
    val nonEmptySerialized = newSerde.toBytes(nonEmpty)
    newSerde.fromBytes(nonEmptySerialized) should equal(nonEmpty)
  }

  "Case class types" should "be treated as Named Types" in {
    val emptySerialized = newSerde.toBytes(AvroNamedRecords())
    val empty = newSerde.fromBytes(emptySerialized)
    empty should equal(AvroNamedRecords())

    val a = AvroNamedRecords(SimpleKey(99), Some(SimpleKey(99)), None, List(SimpleKey(99), SimpleKey(100)), List(None, Some(SimpleKey(99)), None))
    val as = newSerde.toBytes(a)
    newSerde.fromBytes(as) should be (a)
  }

  //TODO #32 "Incompatible change" should "result in exception during registration" in


}
