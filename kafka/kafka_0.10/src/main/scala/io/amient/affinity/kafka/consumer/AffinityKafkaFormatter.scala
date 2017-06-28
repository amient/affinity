package io.amient.affinity.kafka.consumer

import java.io.PrintStream

import kafka.common.MessageFormatter
import org.apache.kafka.clients.consumer.ConsumerRecord

class AffinityKafkaFormatter extends MessageFormatter {
  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    //TODO #44 AffinityKafkaFormatter
  }
}
