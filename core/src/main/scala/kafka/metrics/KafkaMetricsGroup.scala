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

import com.codahale.metrics._
import com.codahale.metrics.MetricRegistry.MetricSupplier
import kafka.utils.Logging
import org.apache.kafka.common.utils.Sanitizer

trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new metric name for gauges, meters, etc. created for this
   * metrics group.
   * @param name Descriptive name of the metric.
   * @param tags Additional attributes which mBean will have.
   * @return Sanitized metric name literal.
   */
  def metricName(name: String, tags: scala.collection.Map[String, String]): String = {
    val klass = this.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")

    explicitMetricName(pkg, simpleName, name, tags)
  }


  protected def explicitMetricName(group: String, typeName: String, name: String,
                                   tags: scala.collection.Map[String, String]): String = {

    val nameBuilder: StringBuilder = new StringBuilder

    nameBuilder.append(group)

    nameBuilder.append(":type=")

    nameBuilder.append(typeName)

    if (name.length > 0) {
      nameBuilder.append(",name=")
      nameBuilder.append(name)
    }

    val tagsName = toMBeanName(tags)
    tagsName.foreach(nameBuilder.append(",").append(_))

    nameBuilder.toString
  }

  def newGauge[T](name: String, gauge: () => T, tags: scala.collection.Map[String, String] = Map.empty): Gauge[T] = {
    val supplier = new MetricSupplier[Gauge[_]] {
      override def newMetric(): Gauge[T] = new Gauge[T] {
        override def getValue: T = gauge()
      }
    }
    kafkaMetricRegistry.gauge(metricName(name, tags), supplier).asInstanceOf[Gauge[T]]
  }

  def newMeter(name: String, tags: scala.collection.Map[String, String] = Map.empty): Meter =
    kafkaMetricRegistry.meter(metricName(name, tags))

  def newHistogram(name: String, biased: Boolean = true, tags: scala.collection.Map[String, String] = Map.empty): Histogram = {
    val supplier = new MetricSupplier[Histogram] {
      override def newMetric(): Histogram = {
        //TODO evaluate adding other kind of reservoirs
        val reservoir = if (biased) new ExponentiallyDecayingReservoir() else new UniformReservoir()
        new Histogram(reservoir)
      }
    }
    kafkaMetricRegistry.histogram(metricName(name, tags), supplier)
  }

  def newTimer(name: String, tags: scala.collection.Map[String, String] = Map.empty): Timer =
    kafkaMetricRegistry.timer(metricName(name, tags))

  def removeMetric(name: String, tags: scala.collection.Map[String, String] = Map.empty): Boolean =
    kafkaMetricRegistry.remove(metricName(name, tags))

  private def toMBeanName(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != "" }
    if (filteredTags.nonEmpty) {
      val tagsString = filteredTags.map { case (key, value) => "%s=%s".format(key, Sanitizer.jmxSanitize(value)) }.mkString(",")
      Some(tagsString)
    }
    else None
  }

}

object KafkaMetricsGroup extends KafkaMetricsGroup
