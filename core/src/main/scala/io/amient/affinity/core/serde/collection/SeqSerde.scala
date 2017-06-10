package io.amient.affinity.core.serde.collection

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import akka.actor.ExtendedActorSystem
import io.amient.affinity.core.serde.primitive.AbstractWrapSerde

class SeqSerde(system: ExtendedActorSystem) extends AbstractWrapSerde(system) {

  override def identifier: Int = 141

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = {
    val di = new DataInputStream(new ByteArrayInputStream(bytes))
    val numItems = di.readInt()
    val result = ((1 to numItems) map { _ =>
      val len = di.readInt()
      val item = new Array[Byte](len)
      di.read(item)
      fromBinaryWrapped(item)
    }).toList
    di.close()
    result
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case list: Seq[_] =>
        val os = new ByteArrayOutputStream()
        val d = new DataOutputStream(os)
        d.writeInt(list.size)
        for (a: Any <- list) a match {
          case ref: AnyRef =>
            val item = toBinaryWrapped(ref)
            d.writeInt(item.length)
            d.write(item)
        }
        os.close
        os.toByteArray
    }

  }
}
