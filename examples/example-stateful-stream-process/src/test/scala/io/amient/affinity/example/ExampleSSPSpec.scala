package io.amient.affinity.example

import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.scalatest.{FlatSpec, Matchers}

class ExampleSSPSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {
  override def numPartitions = 1
  //TODO
}
