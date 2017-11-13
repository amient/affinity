package io.amient.affinity.testutil

import com.typesafe.config.{Config, ConfigValueFactory}
import io.amient.affinity.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.core.cluster.CoordinatorZk
import io.amient.affinity.kafka.EmbeddedZooKeeper

trait SystemTestBaseWithZk extends SystemTestBase with EmbeddedZooKeeper {

    override def configure(config: Config): Config = super.configure(config)
      .withValue(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
      .withValue(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))

}
