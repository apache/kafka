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

import com.yammer.metrics.core.{Gauge, Histogram, MetricName}
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

final class ForwardingManagerMetrics extends AutoCloseable {
  import ForwardingManagerMetrics._

  private val metricsGroup: KafkaMetricsGroup = new KafkaMetricsGroup("kafka.server","ForwardingManager")

  /**
   * A histogram describing the amount of time in milliseconds each admin request spends in the broker's forwarding manager queue, waiting to be sent to the controller.
   * This does not include the time that the request spends waiting for a response from the controller.
   */
  val queueTimeMsName: MetricName = metricsGroup.metricName("QueueTimeMs", Collections.emptyMap())
  val queueTimeMsHist: Histogram = KafkaYammerMetrics.defaultRegistry().newHistogram(queueTimeMsName, true)

  /**
   * The current number of RPCs that are waiting in the broker's forwarding manager queue, waiting to be sent to the controller.
   */
  val queueLengthName: MetricName = metricsGroup.metricName("QueueLength", Collections.emptyMap())
  val queueLength: AtomicInteger = new AtomicInteger(0)
  val queueLengthGauge: Gauge[Int] = KafkaYammerMetrics.defaultRegistry().newGauge(queueLengthName, new FuncGauge[Int](queueLength.get))

  /**
   * A histogram describing the amount of time in milliseconds each request sent by the ForwardingManager spends waiting for a response. " +
   *  "This does not include the time spent in the queue.
   */
  val remoteTimeMsName: MetricName = metricsGroup.metricName("RemoteTimeMs", Collections.emptyMap())
  val remoteTimeMsHist: Histogram = KafkaYammerMetrics.defaultRegistry().newHistogram(remoteTimeMsName, true)


  override def close(): Unit = {
    KafkaYammerMetrics.defaultRegistry().removeMetric(queueTimeMsName)
    KafkaYammerMetrics.defaultRegistry().removeMetric(queueLengthName)
    KafkaYammerMetrics.defaultRegistry().removeMetric(remoteTimeMsName)
  }
}

object ForwardingManagerMetrics {
  private final class FuncGauge[T](func: T) extends Gauge[T] {
    override def value(): T = func
  }
}
