/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.{Gauge, MetricName, Meter, Histogram, Timer}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Sanitizer

trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new MetricName object for gauges, meters, etc. created for this
   * metrics group.
   * @param name Descriptive name of the metric.
   * @param tags Additional attributes which mBean will have.
   * @return Sanitized metric name object.
   */
  def metricName(name: String, tags: scala.collection.Map[String, String]): MetricName = {
    val klass = this.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")

    explicitMetricName(pkg, simpleName, name, tags)
  }


  protected def explicitMetricName(group: String, typeName: String, name: String,
                                   tags: scala.collection.Map[String, String]): MetricName = {

    val nameBuilder: StringBuilder = new StringBuilder

    nameBuilder.append(group)

    nameBuilder.append(":type=")

    nameBuilder.append(typeName)

    if (name.length > 0) {
      nameBuilder.append(",name=")
      nameBuilder.append(name)
    }

    val scope: String = toScope(tags).orNull
    val tagsName = toMBeanName(tags)
    tagsName.foreach(nameBuilder.append(",").append(_))

    new MetricName(group, typeName, name, scope, nameBuilder.toString)
  }

  def newGauge[T](name: String, metric: Gauge[T], tags: scala.collection.Map[String, String] = Map.empty): Gauge[T] =
    KafkaYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric)

  def newMeter(name: String, eventType: String, timeUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty): Meter =
    KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit)

  def newHistogram(name: String, biased: Boolean = true, tags: scala.collection.Map[String, String] = Map.empty): Histogram =
    KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased)

  def newTimer(name: String, durationUnit: TimeUnit, rateUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty): Timer =
    KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit)

  def removeMetric(name: String, tags: scala.collection.Map[String, String] = Map.empty): Unit =
    KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags))

  private def toMBeanName(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != "" }
    if (filteredTags.nonEmpty) {
      val tagsString = filteredTags.map { case (key, value) => "%s=%s".format(key, Sanitizer.jmxSanitize(value)) }.mkString(",")
      Some(tagsString)
    }
    else None
  }

  private def toScope(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != ""}
    if (filteredTags.nonEmpty) {
      // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
      val tagsString = filteredTags
        .toList.sortWith((t1, t2) => t1._1 < t2._1)
        .map { case (key, value) => "%s.%s".format(key, value.replaceAll("\\.", "_"))}
        .mkString(".")

      Some(tagsString)
    }
    else None
  }

}

object KafkaMetricsGroup extends KafkaMetricsGroup
