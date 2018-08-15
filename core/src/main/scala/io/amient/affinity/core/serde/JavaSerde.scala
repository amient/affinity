package io.amient.affinity.core.serde

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config

class JavaSerde(tools: Serdes) extends Serde[AnyRef] {

  def this (config: Config) = this(Serde.tools(config))
  def this (system: ExtendedActorSystem) = this(system.settings.config)

  override def toBytes(obj: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(obj)
    out.close()
    bos.toByteArray
  }

  override protected def fromBytes(bytes: Array[Byte]): AnyRef = {
    val in = new ByteArrayInputStream(bytes)
    val is = new ObjectInputStream(in)
    val obj = is.readObject
    in.close()
    obj
  }

  override def identifier: Int = 101

  override def close(): Unit = ()
}
