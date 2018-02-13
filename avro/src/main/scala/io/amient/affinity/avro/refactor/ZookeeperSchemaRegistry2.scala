package io.amient.affinity.avro.refactor

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.AvroSerde
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.apache.zookeeper.CreateMode

class ZookeeperSchemaRegistry2(config: Config) extends AvroSchemaRegistry2 {

  val merged = config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path))
  val conf = new ZkAvroConf().apply(merged)
  private val zkRoot = conf.Root()

  private val zk = new ZkClient(conf.Connect(), conf.SessionTimeoutMs(), conf.ConnectTimeoutMs(), new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes
    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  })

  private val validator = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()

  private val zkSchemas = s"$zkRoot/schemas"

  private val zkSubjects = s"$zkRoot/subjects"

  override def close(): Unit = zk.close()

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = new Schema.Parser().parse(zk.readData[String](s"$zkSchemas/$id"))

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = hypersynchronized {
    val versions: Map[Schema, Int] = zk.readData[String](s"$zkSubjects/subject") match {
      case null => Map.empty
      case some => some.split(",").toList.map(_.toInt).map {
        case id => getSchema(id) -> id
      }.toMap
    }
    versions.get(schema) match {
      case Some(id) => id
      case None => zk.create(s"$zkSchemas/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL).substring(zkSchemas.length + 1).toInt
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
