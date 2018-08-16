package io.amient.affinity.core.serde

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}

import akka.actor.ExtendedActorSystem
import akka.serialization.JavaSerializer
import akka.util.ClassLoaderObjectInputStream

class JavaSerde(system: ExtendedActorSystem) extends Serde[AnyRef] {

  override def identifier: Int = 101

  override def close(): Unit = ()

  override def fromBytes(bytes: Array[Byte]): AnyRef = {
    val in = new ClassLoaderObjectInputStream(system.dynamicAccess.classLoader, new ByteArrayInputStream(bytes))
    val obj = JavaSerializer.currentSystem.withValue(system) { in.readObject }
    in.close()
    obj
  }

  override def toBytes(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    JavaSerializer.currentSystem.withValue(system) { out.writeObject(o) }
    out.close()
    bos.toByteArray
  }

}
