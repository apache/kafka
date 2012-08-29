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
   * This method enables the user to form logical sub-groups of this
   * KafkaMetricsGroup by inserting a sub-group identifier in the package
   * string.
   *
   * @return The sub-group identifier.
   */
  def metricsGroupIdent: String = ""

  /**
   * Creates a new MetricName object for gauges, meters, etc. created for this
   * metrics group. It uses the metricsGroupIdent to create logical sub-groups.
   * This is currently specifically of use to classes under kafka, with
   * broker-id being the most common metrics grouping strategy.
   *
   * @param name Descriptive name of the metric.
   * @return Sanitized metric name object.
   */
  private def metricName(name: String) = {
    val ident = metricsGroupIdent
    val klass = this.getClass
    val pkg = {
      val actualPkg = if (klass.getPackage == null) "" else klass.getPackage.getName
      if (ident.nonEmpty) {
        // insert the sub-group identifier after the top-level package
        if (actualPkg.contains("."))
          actualPkg.replaceFirst("""\.""", ".%s.".format(ident))
        else
          actualPkg + "." + ident
      }
      else
        actualPkg
    }
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")
    new MetricName(pkg, simpleName, name)
  }

  def newGauge[T](name: String, metric: Gauge[T]) =
    Metrics.newGauge(metricName(name), metric)

  def newMeter(name: String, eventType: String, timeUnit: TimeUnit) =
    Metrics.newMeter(metricName(name), eventType, timeUnit)

  def newHistogram(name: String, biased: Boolean = false) = Metrics.newHistogram(metricName(name), biased)

  def newTimer(name: String, durationUnit: TimeUnit, rateUnit: TimeUnit) =
    Metrics.newTimer(metricName(name), durationUnit, rateUnit)

}
