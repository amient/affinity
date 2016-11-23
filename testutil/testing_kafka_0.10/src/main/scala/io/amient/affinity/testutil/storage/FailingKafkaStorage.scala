package io.amient.affinity.testutil.storage

import java.nio.ByteBuffer
import java.util.concurrent.{Callable, Executors, Future}

import com.typesafe.config.Config
import io.amient.affinity.core.storage.kafka.KafkaStorage
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * This class is for simulating write failures in the KafkaStorage
  * @param config
  * @param partition
  */
class FailingKafkaStorage(config: Config, partition: Int) extends KafkaStorage(config, partition) {

  val executor = Executors.newFixedThreadPool(1)

  override def write(key: ByteBuffer, value: ByteBuffer): Future[RecordMetadata] = {

    val javaFuture: Future[RecordMetadata] = kafkaProducer.send(new ProducerRecord(topic, partition, key, value))
    return executor.submit(new Callable[RecordMetadata]() {
      override def call(): RecordMetadata = {
        if (System.currentTimeMillis() % 10 == 0) {
          throw new RuntimeException("Simulated Exception in FailingKafkaStorage")
        } else {
          javaFuture.get
        }
      }
    })
  }

}
