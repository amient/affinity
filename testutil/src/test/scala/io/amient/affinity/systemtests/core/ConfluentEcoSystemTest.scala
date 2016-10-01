package io.amient.affinity.systemtests.core

import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import org.apache.avro.SchemaBuilder

//TODO #16 ConfluentEcoSystemTest
class ConfluentEcoSystemTest extends SystemTestBaseWithConfluentRegistry {

  val id = registryClient.register("topic-key", SchemaBuilder.builder().intType())
  val meta = registryClient.getLatestSchemaMetadata("topic-key")
  println(meta.getVersion)
  println(meta.getSchema)

}