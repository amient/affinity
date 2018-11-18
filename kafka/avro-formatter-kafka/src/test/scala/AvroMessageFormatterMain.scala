import java.time.temporal.ChronoUnit
import java.util.Properties

import io.amient.affinity.kafka.AvroMessageFormatter
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._

object AvroMessageFormatterMain extends App {

  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", args(0))
  consumerProps.put("security.protocol", "SASL_SSL")
  consumerProps.put("sasl.mechanism", "PLAIN")
  consumerProps.put("client.id", "console")
  consumerProps.put("group.id", "console")
  consumerProps.put("key.deserializer", classOf[ByteArrayDeserializer])
  consumerProps.put("value.deserializer", classOf[ByteArrayDeserializer])
  consumerProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + args(1) + "' password='" + args(2) + "';")
  consumerProps.put("schema.registry.url", args(3))
  val c = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  c.subscribe(List(args(4)).asJava)
  val formatter = new AvroMessageFormatter()
  formatter.init(consumerProps)
  while (true) {
    c.poll(1000).asScala.foreach {
      record => formatter.writeTo(record, System.out)
    }
  }

}
