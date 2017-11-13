package io.amient.affinity.avro

import io.amient.affinity.kafka.EmbeddedCfRegistry
import org.scalatest.{FlatSpec, Matchers}

class CfAvroSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedCfRegistry {

  override def numPartitions = 1

  //TODO port existing core/CfSchemaRegistrySpec to this module
  //TODO #32 "CfAvroSchemaRegistry" should "reject incompatible schema for an existing type"
}
