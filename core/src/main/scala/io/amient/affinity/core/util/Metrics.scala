package io.amient.affinity.core.util

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import io.amient.affinity.Conf

object Metrics extends MetricRegistry {

  def enableJmx(conf: Conf): Unit = {
    JmxReporter.forRegistry(this).inDomain(conf.Affi.Node.SystemName()).build().start()
  }

}
