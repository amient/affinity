package io.amient.affinity.kafka

import java.util

import io.amient.affinity.core.Murmur2Partitioner
import org.apache.kafka.common.{Cluster, Node, PartitionInfo}
import org.scalatest.{FlatSpec, Matchers}

class KafkaPartitionerSpec extends FlatSpec with Matchers {

  behavior of "kafka.DefaultPartitioner"

  it should "have identical method to Murmur2Partitioner" in {
    val kafkaPartitioner = new org.apache.kafka.clients.producer.internals.DefaultPartitioner()
    val affinityPartitioner = new Murmur2Partitioner
    val value = "test-value-for-partitioner"
    val bytes: Array[Byte] = value.getBytes
    val kafkaP = kafkaPartitioner.partition("test", value, bytes, value, bytes, new Cluster("mock-cluster",
      util.Arrays.asList[Node](),
      util.Arrays.asList(
        new PartitionInfo("test", 0, null, Array(), Array()),
        new PartitionInfo("test", 1, null, Array(), Array()),
        new PartitionInfo("test", 2, null, Array(), Array()),
        new PartitionInfo("test", 3, null, Array(), Array())
      ),
      new util.HashSet[String],
      new util.HashSet[String]))
    val affinityP = affinityPartitioner.partition(bytes, 4)
    kafkaP should equal(affinityP)
  }

}
