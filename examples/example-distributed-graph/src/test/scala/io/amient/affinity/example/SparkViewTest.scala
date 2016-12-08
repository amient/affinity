package io.amient.affinity.spark

import java.io._
import java.nio.ByteBuffer
import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.util.ByteUtils
import io.amient.affinity.kafka.KafkaClientImpl
import io.amient.util.spark.KafkaRDD
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SparkViewTest extends App {

  val impl = new KafkaClientImpl("graph", new Properties() {
    put("bootstrap.servers", "localhost:9092,localhost:9091")
  })

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("Affinity_Spark_2.0")
//    .set("spark.serializer", classOf[SparkAkkaSerde].getName)

  val sc = new SparkContext(conf)

  val system = ActorSystem(UUID.randomUUID().toString, ConfigFactory.load("example"))
  val avroserde = SerializationExtension(system).serializerByIdentity(101)

  val rdd = new KafkaRDD(sc, impl).compacted.mapPartitions { part =>
    part.map { case (k, v) =>
      println(avroserde.fromBinary(v.bytes))
      (k, v) //avroserde.fromBinary(v.bytes))
    }
  }
  rdd.collect().foreach(println)

  system.terminate()

}

class SparkAkkaSerde extends Serializer /*with Externalizable*/ {


  override def newInstance(): SerializerInstance = new SerializerInstance {

//    val system = ActorSystem(UUID.randomUUID().toString, ConfigFactory.load("example"))
//    val serdesystem = SerializationExtension(system)

    override def serialize[T: ClassTag](t: T): ByteBuffer = {
      val bos = new ByteArrayOutputStream()
      val out = serializeStream(bos)
      out.writeObject(t)
      out.close()
      ByteBuffer.wrap(bos.toByteArray)
    }

    override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
      val bis = new ByteArrayInputStream(ByteUtils.bufToArray(bytes))
      val in = deserializeStream(bis)
      val result = in.readObject()
      in.close()
      result
    }

    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
      deserialize(bytes)
    }

    override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
      override def writeObject[T: ClassTag](t: T): SerializationStream = {
        val r = t.asInstanceOf[AnyRef]
        val s = serdesystem.findSerializerFor(r)
        ByteUtils.writeIntValue(s.identifier, out)
        val bytes = s.toBinary(r)
        ByteUtils.writeIntValue(bytes.length, out)
        out.write(bytes)
        this
      }

      override def flush(): Unit = ()

      override def close(): Unit = ()
    }

    override def deserializeStream(in: InputStream): DeserializationStream = new DeserializationStream {

      override def readObject[T: ClassTag](): T = {
        val id = ByteUtils.readIntValue(in)
        val s = serdesystem.serializerByIdentity(id)
        val bytes = new Array[Byte](ByteUtils.readIntValue(in))
        in.read(bytes, 0, bytes.length)
        s.fromBinary(bytes).asInstanceOf[T]
      }

      override def close() = ()
    }
  }
}