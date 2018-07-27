package io.amient.affinity.core.state

import com.typesafe.config.Config

object KVStoreConf extends StateConf {
  override def apply(config: Config): StateConf = new StateConf().apply(config)
}
