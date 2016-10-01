package io.amient.affinity.testutil.storage

import java.nio.ByteBuffer

import com.typesafe.config.Config
import io.amient.affinity.core.storage.kafka.KafkaStorage
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * This class is for simulating write failures in the KafkaStorage
  * @param config
  * @param partition
  */
class FailingKafkaStorage(config: Config, partition: Int) extends KafkaStorage(config, partition) {

  override def write(key: ByteBuffer, value: ByteBuffer): Future[RecordMetadata] = {
    kafkaProducer.send(new ProducerRecord(topic, partition, key, value)) match {
      case jfuture => Future {
        if (System.currentTimeMillis() % 10 == 0) throw new RuntimeException("Simulated Exception in FailingKafkaStorage")
        jfuture.get
      }
    }
  }
}
