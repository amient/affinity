package io.amient.affinity.kafka

import java.util.concurrent.Future

import io.amient.affinity.core.storage.{LogStorageConf, Record}
import io.amient.affinity.core.util.MappedJavaFuture
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * This class is for simulating write failures in the KafkaStorage
  */
class FailingKafkaStorage(conf: LogStorageConf) extends KafkaLogStorage(conf) {

  override def append(record: Record[Array[Byte], Array[Byte]]): Future[java.lang.Long] = {
    val producerRecord = new ProducerRecord(topic, null, record.timestamp, record.key, record.value)
    new MappedJavaFuture[RecordMetadata, java.lang.Long](producer.send(producerRecord)) {
      override def map(result: RecordMetadata): java.lang.Long = {
        if (System.currentTimeMillis() % 3 == 0) throw new RuntimeException("simulated kafka producer error")
        result.offset()
      }
    }
  }

}
