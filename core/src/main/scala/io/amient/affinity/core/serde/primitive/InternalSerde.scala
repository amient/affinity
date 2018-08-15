package io.amient.affinity.core.serde.primitive

import java.io._

import akka.actor.{ActorRef, ExtendedActorSystem}
import com.typesafe.config.Config
import io.amient.affinity.core.actor.{CreateKeyValueMediator, KeyValueMediatorCreated, RegisterMediatorSubscriber}
import io.amient.affinity.core.serde.{AbstractWrapSerde, Serde, Serdes}
import io.amient.affinity.core.state._

trait InternalMessage extends Serializable

class InternalSerde(serdes: Serdes) extends AbstractWrapSerde(serdes) with Serde[InternalMessage] {

  def this(system: ExtendedActorSystem) = this(Serde.tools(system))
  def this(config: Config) = this(Serde.tools(config))

  override def identifier: Int = 99

  //partition mediator messages
  val CreateKeyValueMediatorManifest = 1
  val KeyValueMediatorCreatedManifest = 2
  val RegisterMediatorSubscriberManifest = 3

  //global kv store messages
  val ReplaceManifest = 11
  val DeleteManifest = 12
  val InsertManifest = 13
  val GetAndUpdateManifest = 14
  val UpdateAndGetManifest = 15


  override def toBytes(obj: InternalMessage): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new DataOutputStream(bos)
    obj match {
      case CreateKeyValueMediator(key, value) =>
        out.writeByte(CreateKeyValueMediatorManifest)
        out.writeUTF(key)
        write(out, value)
      case KeyValueMediatorCreated(ref) =>
        out.writeByte(KeyValueMediatorCreatedManifest)
        write(out, ref)
      case RegisterMediatorSubscriber(ref) =>
        out.writeByte(RegisterMediatorSubscriberManifest)
        write(out, ref)
      case KVGReplace(key, value) =>
        out.writeByte(ReplaceManifest)
        write(out, key)
        write(out, value)
      case KVGDelete(key) =>
        out.writeByte(DeleteManifest)
        write(out, key)
      case KVGInsert(key, value) =>
        out.writeByte(InsertManifest)
        write(out, key)
        write(out, value)
      case KVGGetAndUpdate(key, f) =>
        out.writeByte(GetAndUpdateManifest)
        write(out, key)
        write(out, f)
      case KVGUpdateAndGet(key, f) =>
        out.writeByte(UpdateAndGetManifest)
        write(out, key)
        write(out, f)
    }
    out.close()
    bos.toByteArray
  }

  override protected def fromBytes(bytes: Array[Byte]): InternalMessage = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      in.readByte() match {
        case CreateKeyValueMediatorManifest => CreateKeyValueMediator(in.readUTF(), read(in))
        case KeyValueMediatorCreatedManifest => KeyValueMediatorCreated(read(in).asInstanceOf[ActorRef])
        case RegisterMediatorSubscriberManifest => RegisterMediatorSubscriber(read(in).asInstanceOf[ActorRef])
        case ReplaceManifest => KVGReplace(read(in), read(in))
        case DeleteManifest => KVGDelete(read(in))
        case InsertManifest => KVGInsert(read(in), read(in))
        case GetAndUpdateManifest => KVGGetAndUpdate(read(in), read(in).asInstanceOf[Option[Any] => Option[Any]])
        case UpdateAndGetManifest => KVGUpdateAndGet(read(in), read(in).asInstanceOf[Option[Any] => Option[Any]])
      }
    } finally {
      in.close()
    }
  }

  override def close(): Unit = ()

  private def write(out: DataOutputStream, data: Any): Unit = {
    val binary = toBinaryWrapped(data, 0)
    out.writeShort(binary.length)
    out.write(binary)
  }

  private def read(in: DataInputStream): Any = {
    val length = in.readShort().toInt
    val binary = new Array[Byte](length)
    in.readFully(binary)
    fromBinaryWrapped(binary)
  }


}
