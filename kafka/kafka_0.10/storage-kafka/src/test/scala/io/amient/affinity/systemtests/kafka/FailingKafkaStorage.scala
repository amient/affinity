package io.amient.affinity.systemtests.kafka

import java.util.concurrent.Future

import io.amient.affinity.core.storage.StateConf
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.MappedJavaFuture
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * This class is for simulating write failures in the KafkaStorage
  * @param conf
  * @param partition
  */
class FailingKafkaStorage(conf: StateConf, partition: Int, numParts: Int) extends KafkaStorage(conf, partition, numParts) {

  override def write(key: Array[Byte], value: Array[Byte], timestamp: Long): Future[java.lang.Long] = {
    new MappedJavaFuture[RecordMetadata, java.lang.Long](kafkaProducer.send(new ProducerRecord(topic, partition, timestamp, key, value))) {
      override def map(result: RecordMetadata): java.lang.Long = result.offset()
    }
  }

}
