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


import java.util.concurrent.TimeUnit

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.consumer.{ConsumerTopicStatsRegistry, FetchRequestAndResponseStatsRegistry}
import kafka.producer.{ProducerRequestStatsRegistry, ProducerStatsRegistry, ProducerTopicStatsRegistry}
import kafka.utils.Logging

import scala.collection.immutable


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

  def removeMetric(name: String) =
    Metrics.defaultRegistry().removeMetric(metricName(name))

}

object KafkaMetricsGroup extends KafkaMetricsGroup with Logging {
  /**
   * To make sure all the metrics be de-registered after consumer/producer close, the metric names should be
   * put into the metric name set.
   */
  private val consumerMetricNameList: immutable.List[MetricName] = immutable.List[MetricName](
    // kafka.consumer.ZookeeperConsumerConnector
    new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "-FetchQueueSize"),
    new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "-KafkaCommitsPerSec"),
    new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "-ZooKeeperCommitsPerSec"),
    new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "-RebalanceRateAndTime"),

    // kafka.consumer.ConsumerFetcherManager
    new MetricName("kafka.consumer", "ConsumerFetcherManager", "-MaxLag"),
    new MetricName("kafka.consumer", "ConsumerFetcherManager", "-MinFetchRate"),

    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
    new MetricName("kafka.server", "FetcherLagMetrics", "-ConsumerLag"),

    // kafka.consumer.ConsumerTopicStats <-- kafka.consumer.{ConsumerIterator, PartitionTopicInfo}
    new MetricName("kafka.consumer", "ConsumerTopicMetrics", "-MessagesPerSec"),
    new MetricName("kafka.consumer", "ConsumerTopicMetrics", "-AllTopicsMessagesPerSec"),

    // kafka.consumer.ConsumerTopicStats
    new MetricName("kafka.consumer", "ConsumerTopicMetrics", "-BytesPerSec"),
    new MetricName("kafka.consumer", "ConsumerTopicMetrics", "-AllTopicsBytesPerSec"),

    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
    new MetricName("kafka.server", "FetcherStats", "-BytesPerSec"),
    new MetricName("kafka.server", "FetcherStats", "-RequestsPerSec"),

    // kafka.consumer.FetchRequestAndResponseStats <-- kafka.consumer.SimpleConsumer
    new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "-FetchResponseSize"),
    new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "-FetchRequestRateAndTimeMs"),
    new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "-AllBrokersFetchResponseSize"),
    new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "-AllBrokersFetchRequestRateAndTimeMs"),

    /**
     * ProducerRequestStats <-- SyncProducer
     * metric for SyncProducer in fetchTopicMetaData() needs to be removed when consumer is closed.
     */
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-ProducerRequestRateAndTimeMs"),
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-ProducerRequestSize"),
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-AllBrokersProducerRequestRateAndTimeMs"),
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-AllBrokersProducerRequestSize")
  )

  private val producerMetricNameList: immutable.List[MetricName] = immutable.List[MetricName] (
    // kafka.producer.ProducerStats <-- DefaultEventHandler <-- Producer
    new MetricName("kafka.producer", "ProducerStats", "-SerializationErrorsPerSec"),
    new MetricName("kafka.producer", "ProducerStats", "-ResendsPerSec"),
    new MetricName("kafka.producer", "ProducerStats", "-FailedSendsPerSec"),

    // kafka.producer.ProducerSendThread
    new MetricName("kafka.producer.async", "ProducerSendThread", "-ProducerQueueSize"),

    // kafka.producer.ProducerTopicStats <-- kafka.producer.{Producer, async.DefaultEventHandler}
    new MetricName("kafka.producer", "ProducerTopicMetrics", "-MessagesPerSec"),
    new MetricName("kafka.producer", "ProducerTopicMetrics", "-DroppedMessagesPerSec"),
    new MetricName("kafka.producer", "ProducerTopicMetrics", "-BytesPerSec"),
    new MetricName("kafka.producer", "ProducerTopicMetrics", "-AllTopicsMessagesPerSec"),
    new MetricName("kafka.producer", "ProducerTopicMetrics", "-AllTopicsDroppedMessagesPerSec"),
    new MetricName("kafka.producer", "ProducerTopicMetrics", "-AllTopicsBytesPerSec"),

    // kafka.producer.ProducerRequestStats <-- SyncProducer
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-ProducerRequestRateAndTimeMs"),
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-ProducerRequestSize"),
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-AllBrokersProducerRequestRateAndTimeMs"),
    new MetricName("kafka.producer", "ProducerRequestMetrics", "-AllBrokersProducerRequestSize")
  )

  def removeAllConsumerMetrics(clientId: String) {
    FetchRequestAndResponseStatsRegistry.removeConsumerFetchRequestAndResponseStats(clientId)
    ConsumerTopicStatsRegistry.removeConsumerTopicStat(clientId)
    ProducerRequestStatsRegistry.removeProducerRequestStats(clientId)
    removeAllMetricsInList(KafkaMetricsGroup.consumerMetricNameList, clientId)
  }

  def removeAllProducerMetrics(clientId: String) {
    ProducerRequestStatsRegistry.removeProducerRequestStats(clientId)
    ProducerTopicStatsRegistry.removeProducerTopicStats(clientId)
    ProducerStatsRegistry.removeProducerStats(clientId)
    removeAllMetricsInList(KafkaMetricsGroup.producerMetricNameList, clientId)
  }

  private def removeAllMetricsInList(metricNameList: immutable.List[MetricName], clientId: String) {
    metricNameList.foreach(metric => {
      val pattern = (clientId + ".*" + metric.getName +".*").r
      val registeredMetrics = scala.collection.JavaConversions.asScalaSet(Metrics.defaultRegistry().allMetrics().keySet())
      for (registeredMetric <- registeredMetrics) {
        if (registeredMetric.getGroup == metric.getGroup &&
          registeredMetric.getType == metric.getType) {
          pattern.findFirstIn(registeredMetric.getName) match {
            case Some(_) => {
              val beforeRemovalSize = Metrics.defaultRegistry().allMetrics().keySet().size
              Metrics.defaultRegistry().removeMetric(registeredMetric)
              val afterRemovalSize = Metrics.defaultRegistry().allMetrics().keySet().size
              trace("Removing metric %s. Metrics registry size reduced from %d to %d".format(
                  registeredMetric, beforeRemovalSize, afterRemovalSize))
            }
            case _ =>
          }
        }
      }
    })
  }
}
