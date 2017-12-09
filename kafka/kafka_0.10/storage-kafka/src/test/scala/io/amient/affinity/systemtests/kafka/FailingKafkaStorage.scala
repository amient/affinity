package io.amient.affinity.systemtests.kafka

import java.util.concurrent.{CompletableFuture, Future}
import java.util.function.Supplier

import com.typesafe.config.Config
import io.amient.affinity.core.storage.kafka.KafkaStorage
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * This class is for simulating write failures in the KafkaStorage
  * @param config
  * @param partition
  */
class FailingKafkaStorage(config: Config, partition: Int) extends KafkaStorage(config, partition) {

  override def write(key: Array[Byte], value: Array[Byte], timestamp: Long): Future[java.lang.Long] = {
    val javaFuture: Future[RecordMetadata] = kafkaProducer.send(new ProducerRecord(topic, partition, timestamp, key, value))
    CompletableFuture.supplyAsync(new Supplier[java.lang.Long] {
      override def get() = javaFuture.get.offset()
    })
  }

}
