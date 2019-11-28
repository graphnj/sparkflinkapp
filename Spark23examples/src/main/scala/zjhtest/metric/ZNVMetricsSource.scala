package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source
import zjhtest.metric.ZNVStatInfo


/**
  * @auth zhujinhua 0049003202
  * @date 2019/11/9 20:09
  */

private[spark] class ZNVMetricsSource extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "zjhmetrictest"

  //val stat: ZNVStatInfo
  // Gauge for worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("zjh_aaa"), new Gauge[Int] {
    override def getValue: Int = 111//stat.aaa
  })

  // Gauge for alive worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("zjh_bbb"), new Gauge[Int]{
    override def getValue: Int = 222//stat.bbb
  })

  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("zjh_ccc1"), new Gauge[Int] {
    override def getValue: Int = 33//stat.ccc
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("zjh_ccc2"), new Gauge[Int] {
    override def getValue: Int = 555//stat.ccc
  })
}
