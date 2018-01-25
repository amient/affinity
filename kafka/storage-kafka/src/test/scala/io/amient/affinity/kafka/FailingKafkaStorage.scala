package io.amient.affinity.kafka

import java.util.concurrent.Future

import io.amient.affinity.core.storage.StateConf
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.MappedJavaFuture
import io.amient.affinity.stream.Record
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * This class is for simulating write failures in the KafkaStorage
  */
class FailingKafkaStorage(id: String, conf: StateConf, partition: Int, numParts: Int) extends KafkaStorage(id, conf, partition, numParts) {

  override def write(record: Record[Array[Byte], Array[Byte]]): Future[java.lang.Long] = {
    val producerRecord = new ProducerRecord(topic, partition, record.timestamp, record.key, record.value)
    new MappedJavaFuture[RecordMetadata, java.lang.Long](kafkaProducer.send(producerRecord)) {
      override def map(result: RecordMetadata): java.lang.Long = {
        if (System.currentTimeMillis() % 3 == 0) throw new RuntimeException("simulated kafka producer error")
        result.offset()
      }
    }
  }

}
