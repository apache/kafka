/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{Gauge, MetricConfig, Metrics}
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing
import org.apache.kafka.common.metrics.stats.{Percentile, Percentiles}

import java.util.concurrent.atomic.AtomicInteger

final class ForwardingManagerMetrics private (
  metrics: Metrics,
  timeoutMs: Long,
) extends AutoCloseable {
  import ForwardingManagerMetrics._

  /**
   * A histogram describing the amount of time in milliseconds each admin request spends in the broker's forwarding manager queue, waiting to be sent to the controller.
   * This does not include the time that the request spends waiting for a response from the controller.
   */
  val queueTimeMsHist: LatencyHistogram = new LatencyHistogram(metrics, queueTimeMsName, metricGroupName, timeoutMs)

  /**
   * A histogram describing the amount of time in milliseconds each request sent by the ForwardingManager spends waiting for a response.
   * This does not include the time spent in the queue.
   */
  val remoteTimeMsHist: LatencyHistogram = new LatencyHistogram(metrics, remoteTimeMsName, metricGroupName, timeoutMs)

  val queueLengthName: MetricName = metrics.metricName(
    "QueueLength",
    metricGroupName,
    "The current number of RPCs that are waiting in the broker's forwarding manager queue, waiting to be sent to the controller."
  )
  val queueLength: AtomicInteger = new AtomicInteger(0)
  metrics.addMetric(queueLengthName, new FuncGauge(_ => queueLength.get()))

  override def close(): Unit = {
    queueTimeMsHist.close()
    remoteTimeMsHist.close()
    metrics.removeMetric(queueLengthName)
  }
}

object ForwardingManagerMetrics {

  val metricGroupName = "ForwardingManager"
  val queueTimeMsName = "QueueTimeMs"
  val remoteTimeMsName = "RemoteTimeMs"

  final class LatencyHistogram (
    metrics: Metrics,
    name: String,
    group: String,
    maxLatency: Long
  ) extends AutoCloseable {
    private val sensor = metrics.sensor(name)
    val latencyP99Name: MetricName = metrics.metricName(s"$name.p99", group)
    val latencyP999Name: MetricName = metrics.metricName(s"$name.p999", group)

    sensor.add(new Percentiles(
      4000,
      maxLatency,
      BucketSizing.CONSTANT,
      new Percentile(latencyP99Name, 99),
      new Percentile(latencyP999Name, 99.9)
    ))

    override def close(): Unit = {
      metrics.removeSensor(name)
      metrics.removeMetric(latencyP99Name)
      metrics.removeMetric(latencyP999Name)
    }

    def record(latencyMs: Long): Unit = sensor.record(latencyMs)
  }

  private final class FuncGauge[T](func: Long => T) extends Gauge[T] {
    override def value(config: MetricConfig, now: Long): T = {
      func(now)
    }
  }

  def apply(metrics: Metrics, timeoutMs: Long): ForwardingManagerMetrics = new ForwardingManagerMetrics(metrics, timeoutMs)
}
