package io.amient.affinity.core.serde.avro

import akka.actor.ExtendedActorSystem
import io.amient.affinity.core.serde.Serde

final class AvroSerdeProxy(system: ExtendedActorSystem) extends Serde[Any] {

  val config = system.settings.config

  val internal = AvroSerde.create(system.settings.config)

  override def fromBytes(bytes: Array[Byte]): Any = internal.fromBytes(bytes)

  override def toBytes(obj: Any): Array[Byte] = internal.toBytes(obj)

  override def close(): Unit = if (internal != null) internal.close()

  override def identifier: Int = 101

}

