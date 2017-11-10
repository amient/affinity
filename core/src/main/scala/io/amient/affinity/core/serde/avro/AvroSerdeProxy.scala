package io.amient.affinity.core.serde.avro

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import io.amient.affinity.core.serde.{Serde, Serdes}

import scala.util.Try

final class AvroSerdeProxy(tools: Serdes) extends Serde[Any] {

  def this(config: Config) = this(Serde.tools(config))

  def this(system: ExtendedActorSystem) = this(system.settings.config)

  val internal = AvroSerde.create(tools.config)

  override def fromBytes(bytes: Array[Byte]): Any = internal.fromBytes(bytes)

  override def toBytes(obj: Any): Array[Byte] = internal.toBytes(obj)

  override def close(): Unit = if (internal != null) internal.close()

  override def identifier: Int = 200

}

