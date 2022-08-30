/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.metadata

import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.MetricConfig

final class BrokerServerMetrics private (metrics: Metrics) extends AutoCloseable {
  import BrokerServerMetrics._

  val lastAppliedRecordOffset: AtomicLong = new AtomicLong(0)
  val lastAppliedRecordTimestamp: AtomicLong = new AtomicLong(0)
  val metadataLoadErrorCount: AtomicLong = new AtomicLong(0)
  val metadataApplyErrorCount: AtomicLong = new AtomicLong(0)

  val lastAppliedRecordOffsetName = metrics.metricName(
    "last-applied-record-offset",
    metricGroupName,
    "The offset of the last record from the cluster metadata partition that was applied by the broker"
  )

  val lastAppliedRecordTimestampName = metrics.metricName(
    "last-applied-record-timestamp",
    metricGroupName,
    "The timestamp of the last record from the cluster metadata partition that was applied by the broker"
  )

  val lastAppliedRecordLagMsName = metrics.metricName(
    "last-applied-record-lag-ms",
    metricGroupName,
    "The difference between now and the timestamp of the last record from the cluster metadata partition that was applied by the broker"
  )

  val metadataLoadErrorCountName = metrics.metricName(
    "metadata-load-error-count",
    metricGroupName,
    "The number of errors encountered by the BrokerMetadataListener while loading the metadata log and generating a new MetadataDelta based on it."
  )

  val metadataApplyErrorCountName = metrics.metricName(
    "metadata-apply-error-count",
    metricGroupName,
    "The number of errors encountered by the BrokerMetadataPublisher while applying a new MetadataImage based on the latest MetadataDelta."
  )

  addMetric(metrics, lastAppliedRecordOffsetName) { _ =>
    lastAppliedRecordOffset.get
  }

  addMetric(metrics, lastAppliedRecordTimestampName) { _ =>
    lastAppliedRecordTimestamp.get
  }

  addMetric(metrics, lastAppliedRecordLagMsName) { now =>
    now - lastAppliedRecordTimestamp.get
  }

  addMetric(metrics, metadataLoadErrorCountName) { _ =>
    metadataLoadErrorCount.get
  }

  addMetric(metrics, metadataApplyErrorCountName) { _ =>
    metadataApplyErrorCount.get
  }

  override def close(): Unit = {
    List(
      lastAppliedRecordOffsetName,
      lastAppliedRecordTimestampName,
      lastAppliedRecordLagMsName,
      metadataLoadErrorCountName,
      metadataApplyErrorCountName
    ).foreach(metrics.removeMetric)
  }
}


final object BrokerServerMetrics {
  private val metricGroupName = "broker-metadata-metrics"

  private def addMetric[T](metrics: Metrics, name: MetricName)(func: Long => T): Unit = {
    metrics.addMetric(name, new FuncGauge(func))
  }

  private final class FuncGauge[T](func: Long => T) extends Gauge[T] {
    override def value(config: MetricConfig, now: Long): T = {
      func(now)
    }
  }

  def apply(metrics: Metrics): BrokerServerMetrics = {
    new BrokerServerMetrics(metrics)
  }
}
