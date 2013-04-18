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

package kafka.metrics


import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.utils.Logging
import java.util.concurrent.TimeUnit
import com.yammer.metrics.Metrics


trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new MetricName object for gauges, meters, etc. created for this
   * metrics group.
   * @param name Descriptive name of the metric.
   * @return Sanitized metric name object.
   */
  private def metricName(name: String) = {
    val klass = this.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")
    new MetricName(pkg, simpleName, name)
  }

  def newGauge[T](name: String, metric: Gauge[T]) =
    Metrics.defaultRegistry().newGauge(metricName(name), metric)

  def newMeter(name: String, eventType: String, timeUnit: TimeUnit) =
    Metrics.defaultRegistry().newMeter(metricName(name), eventType, timeUnit)

  def newHistogram(name: String, biased: Boolean = true) =
    Metrics.defaultRegistry().newHistogram(metricName(name), biased)

  def newTimer(name: String, durationUnit: TimeUnit, rateUnit: TimeUnit) =
    Metrics.defaultRegistry().newTimer(metricName(name), durationUnit, rateUnit)

}
