package io.amient.affinity.spark

import io.amient.affinity.kafka.EmbeddedKafka
import org.scalatest.{FlatSpec, Matchers}

class CompactRDDTest extends FlatSpec with EmbeddedKafka with Matchers {
  override def numPartitions = 10
  //TODO
}
