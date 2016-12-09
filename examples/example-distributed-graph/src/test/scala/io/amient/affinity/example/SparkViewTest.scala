package io.amient.affinity.spark

import java.util.Properties

import io.amient.affinity.core.serde.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.core.serde.primitive.IntSerde
import io.amient.affinity.example.data.MyProjectAvroSerde
import io.amient.affinity.kafka.KafkaClientImpl
import io.amient.util.spark.KafkaRDD
import org.apache.spark.serializer._
import org.apache.spark.{SparkConf, SparkContext}

class MySparkAvroSerde(zk: String, to1: Int, to2: Int)
  extends ZkAvroSchemaRegistry(zk, to1, to2) with MyProjectAvroSerde


object SparkViewTest extends App {

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("Affinity_Spark_2.0")
    .set("spark.serializer", classOf[KryoSerializer].getName)

  val sc = new SparkContext(conf)

  val impl = new KafkaClientImpl("graph", new Properties() {
    put("bootstrap.servers", "localhost:9092,localhost:9091")
  })

  val rdd = new KafkaRDD(sc, impl).compacted.repartition(1).mapPartitions { part =>
    val intSerde = new IntSerde
    val avroSerde = new MySparkAvroSerde("localhost:2181", 6000, 6000)
    part.map { case (k, v) =>
      (intSerde.fromBytes(k.bytes), avroSerde.fromBytes(v.bytes))
    }
  }
  rdd.collect().foreach(println)

}

