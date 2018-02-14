package io.amient.affinity.avro

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._

object ZookeeperSchemaRegistry {

  object Conf extends Conf

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Avro = struct("affinity.avro", new ZkAvroConf, false)
  }

  class ZkAvroConf extends CfgStruct[ZkAvroConf](classOf[AvroConf]) {
    val Connect = string("schema.registry.zookeeper.connect", true)
    val Root = string("schema.registry.zookeeper.root", true)
    val ConnectTimeoutMs = integer("schema.registry.zookeeper.timeout.connect.ms", true)
    val SessionTimeoutMs = integer("schema.registry.zookeeper.timeout.session.ms", true)
  }
  
}


class ZookeeperSchemaRegistry(zkRoot: String, zk: ZkClient) extends AvroSerde with AvroSchemaRegistry {

  def this(conf: ZkAvroConf) = this(conf.Root(), {
    val zk = new ZkClient(conf.Connect(), conf.SessionTimeoutMs(), conf.ConnectTimeoutMs(), new ZkSerializer {
      def serialize(o: Object): Array[Byte] = o.toString.getBytes

      override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
    })
    val zkRoot = conf.Root()
    if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot)
    val zkSchemas = s"$zkRoot/schemas"
    if (!zk.exists(zkSchemas)) zk.createPersistent(zkSchemas)
    val zkSubjects = s"$zkRoot/subjects"
    if (!zk.exists(zkSubjects)) zk.createPersistent(zkSubjects)
    zk
  })

  def this(config: Config) = this {
    new ZkAvroConf().apply(config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path)))
  }

  override def close(): Unit = zk.close()

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = {
    val zkSchema = s"$zkRoot/schemas/${id.toString.reverse.padTo(10, '0').reverse}"
    if (!zk.exists(zkSchema)) throw new NoSuchElementException else
    new Schema.Parser().parse(zk.readData[String](zkSchema))
  }

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = hypersynchronized {
    val zkSubject = s"$zkRoot/subjects/$subject"
    val zkSchemas = s"$zkRoot/schemas"
    val versions: Map[Schema, Int] =
      if (!zk.exists(zkSubject)) Map.empty else {
        zk.readData[String](zkSubject) match {
          case some => some.split(",").toList.map(_.toInt).map {
            case id => getSchema(id) -> id
          }.toMap
        }
      }
    versions.get(schema).getOrElse {
      validator.validate(schema, versions.map(_._1).asJava)
      val schemaPath = zk.create(s"$zkSchemas/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
      val id = schemaPath .substring(zkSchemas.length + 1).toInt
      val updatedVersions = versions.map(_._2).toList :+ id
      if (zk.exists(zkSubject)) {
        zk.writeData(zkSubject, updatedVersions.mkString(","))
      } else {
        zk.create(zkSubject, updatedVersions.mkString(","), CreateMode.PERSISTENT)
      }
      id
    }
  }


  private def hypersynchronized[X](f: => X): X = synchronized {
    val lockPath = zkRoot + "/lock"
    var acquired = 0
    do {
      try {
        zk.createEphemeral(lockPath)
        acquired = 1
      } catch {
        case _: ZkNodeExistsException =>
          acquired -= 1
          if (acquired < -100) {
            throw new IllegalStateException("Could not acquire zk registry lock")
          } else {
            Thread.sleep(500)
          }
      }
    } while (acquired != 1)
    try f finally zk.delete(lockPath)
  }

}
