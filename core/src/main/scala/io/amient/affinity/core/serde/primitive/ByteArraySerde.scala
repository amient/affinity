package io.amient.affinity.core.serde.primitive

import io.amient.affinity.core.serde.Serde

class ByteArraySerde extends Serde[Array[Byte]] {

  override def identifier = 129

  override def fromBytes(bytes: Array[Byte]) = bytes

  override def toBytes(obj: Array[Byte]) = obj

  override def close() = ()
}
