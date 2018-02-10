package io.amient.affinity.spark

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.SerializerInstance

import scala.reflect.ClassTag

class SparkAvroSerializer(conf: SparkConf) extends org.apache.spark.serializer.Serializer {

  conf.getAll.foreach(println)

  override def newInstance() = new SerializerInstance {

    override def serialize[T](t: T)(implicit evidence$1: ClassTag[T]) = ???

    override def deserialize[T](bytes: ByteBuffer)(implicit evidence$2: ClassTag[T]) = ???

    override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader)(implicit evidence$3: ClassTag[T]) = ???

    override def serializeStream(s: OutputStream) = ???

    override def deserializeStream(s: InputStream) = ???
  }
}
