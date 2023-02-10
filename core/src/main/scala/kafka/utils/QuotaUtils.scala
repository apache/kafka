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

package kafka.utils

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{KafkaMetric, Measurable, QuotaViolationException}
import org.apache.kafka.common.metrics.stats.Rate

/**
 * Helper functions related to quotas
 */
object QuotaUtils {

  /**
   * This calculates the amount of time needed to bring the observed rate within quota
   * assuming that no new metrics are recorded.
   *
   * If O is the observed rate and T is the target rate over a window of W, to bring O down to T,
   * we need to add a delay of X to W such that O * W / (W + X) = T.
   * Solving for X, we get X = (O - T)/T * W.
   *
   * @param timeMs current time in milliseconds
   * @return Delay in milliseconds
   */
  def throttleTime(e: QuotaViolationException, timeMs: Long): Long = {
    val difference = e.value - e.bound
    // Use the precise window used by the rate calculation
    val throttleTimeMs = difference / e.bound * windowSize(e.metric, timeMs)
    Math.round(throttleTimeMs)
  }

  /**
   * Calculates the amount of time needed to bring the observed rate within quota using the same algorithm as
   * throttleTime() utility method but the returned value is capped to given maxThrottleTime
   */
  def boundedThrottleTime(e: QuotaViolationException, maxThrottleTime: Long, timeMs: Long): Long = {
    math.min(throttleTime(e, timeMs), maxThrottleTime)
  }

  /**
   * Returns window size of the given metric
   *
   * @param metric metric with measurable of type Rate
   * @param timeMs current time in milliseconds
   * @throws IllegalArgumentException if given measurable is not Rate
   */
  private def windowSize(metric: KafkaMetric, timeMs: Long): Long =
    measurableAsRate(metric.metricName, metric.measurable).windowSize(metric.config, timeMs)

  /**
   * Casts provided Measurable to Rate
   * @throws IllegalArgumentException if given measurable is not Rate
   */
  private def measurableAsRate(name: MetricName, measurable: Measurable): Rate = {
    measurable match {
      case r: Rate => r
      case _ => throw new IllegalArgumentException(s"Metric $name is not a Rate metric, value $measurable")
    }
  }
}
