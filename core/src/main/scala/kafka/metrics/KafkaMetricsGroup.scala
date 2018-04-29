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

import com.codahale.metrics.Gauge
import kafka.consumer.{ConsumerTopicStatsRegistry, FetchRequestAndResponseStatsRegistry}
import kafka.producer.{ProducerRequestStatsRegistry, ProducerStatsRegistry, ProducerTopicStatsRegistry}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Sanitizer

import scala.collection.immutable
import scala.collection.JavaConverters._


trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new MetricName object for gauges, meters, etc. created for this
   * metrics group.
   * @param name Descriptive name of the metric.
   * @param tags Additional attributes which mBean will have.
   * @return Sanitized metric name object.
   */
  def metricName(name: String, tags: scala.collection.Map[String, String]): String = {
    val klass = this.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")

    explicitMetricName(pkg, simpleName, name, tags)
  }


  protected def explicitMetricName(group: String, typeName: String, name: String,
                                   tags: scala.collection.Map[String, String] = Map.empty): String = {

    val nameBuilder: StringBuilder = new StringBuilder

    nameBuilder.append(group)

    nameBuilder.append(":type=")
    nameBuilder.append(typeName)

    if (name.length > 0) {
      nameBuilder.append(",name=")
      nameBuilder.append(name)
    }

    val tagsName = KafkaMetricsGroup.toMBeanName(tags)
    tagsName.foreach(nameBuilder.append(",").append(_))

    nameBuilder.toString
  }

  def newGauge[T](name: String, metric: Gauge[T], tags: scala.collection.Map[String, String] = Map.empty) =
    kafkaMetricRegistry.register(metricName(name, tags), metric)

  def newMeter(name: String, tags: scala.collection.Map[String, String] = Map.empty) =
    kafkaMetricRegistry.meter(metricName(name, tags))

  def newHistogram(name: String, tags: scala.collection.Map[String, String] = Map.empty) =
    kafkaMetricRegistry.histogram(metricName(name, tags))

  def newTimer(name: String, tags: scala.collection.Map[String, String] = Map.empty) =
    kafkaMetricRegistry.timer(metricName(name, tags))

  def removeMetric(name: String, tags: scala.collection.Map[String, String] = Map.empty) =
    kafkaMetricRegistry.remove(metricName(name, tags))

}

object KafkaMetricsGroup extends KafkaMetricsGroup with Logging {
  /**
   * To make sure all the metrics be de-registered after consumer/producer close, the metric names should be
   * put into the metric name set.
   */
  private val consumerMetricNameList: immutable.List[String] = immutable.List[String](
    // kafka.consumer.ZookeeperConsumerConnector
    explicitMetricName("kafka.consumer", "ZookeeperConsumerConnector", "FetchQueueSize"),
    explicitMetricName("kafka.consumer", "ZookeeperConsumerConnector", "KafkaCommitsPerSec"),
    explicitMetricName("kafka.consumer", "ZookeeperConsumerConnector", "ZooKeeperCommitsPerSec"),
    explicitMetricName("kafka.consumer", "ZookeeperConsumerConnector", "RebalanceRateAndTime"),
    explicitMetricName("kafka.consumer", "ZookeeperConsumerConnector", "OwnedPartitionsCount"),

    // kafka.consumer.ConsumerFetcherManager
    explicitMetricName("kafka.consumer", "ConsumerFetcherManager", "MaxLag"),
    explicitMetricName("kafka.consumer", "ConsumerFetcherManager", "MinFetchRate"),

    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
    explicitMetricName("kafka.server", "FetcherLagMetrics", "ConsumerLag"),

    // kafka.consumer.ConsumerTopicStats <-- kafka.consumer.{ConsumerIterator, PartitionTopicInfo}
    explicitMetricName("kafka.consumer", "ConsumerTopicMetrics", "MessagesPerSec"),

    // kafka.consumer.ConsumerTopicStats
    explicitMetricName("kafka.consumer", "ConsumerTopicMetrics", "BytesPerSec"),

    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
    explicitMetricName("kafka.server", "FetcherStats", "BytesPerSec"),
    explicitMetricName("kafka.server", "FetcherStats", "RequestsPerSec"),

    // kafka.consumer.FetchRequestAndResponseStats <-- kafka.consumer.SimpleConsumer
    explicitMetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchResponseSize"),
    explicitMetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchRequestRateAndTimeMs"),
    explicitMetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchRequestThrottleRateAndTimeMs"),

    /**
     * ProducerRequestStats <-- SyncProducer
     * metric for SyncProducer in fetchTopicMetaData() needs to be removed when consumer is closed.
     */
    explicitMetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestRateAndTimeMs"),
    explicitMetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestSize")
  )

  private val producerMetricNameList: immutable.List[String] = immutable.List[String](
    // kafka.producer.ProducerStats <-- DefaultEventHandler <-- Producer
    explicitMetricName("kafka.producer", "ProducerStats", "SerializationErrorsPerSec"),
    explicitMetricName("kafka.producer", "ProducerStats", "ResendsPerSec"),
    explicitMetricName("kafka.producer", "ProducerStats", "FailedSendsPerSec"),

    // kafka.producer.ProducerSendThread
    explicitMetricName("kafka.producer.async", "ProducerSendThread", "ProducerQueueSize"),

    // kafka.producer.ProducerTopicStats <-- kafka.producer.{Producer, async.DefaultEventHandler}
    explicitMetricName("kafka.producer", "ProducerTopicMetrics", "MessagesPerSec"),
    explicitMetricName("kafka.producer", "ProducerTopicMetrics", "DroppedMessagesPerSec"),
    explicitMetricName("kafka.producer", "ProducerTopicMetrics", "BytesPerSec"),

    // kafka.producer.ProducerRequestStats <-- SyncProducer
    explicitMetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestRateAndTimeMs"),
    explicitMetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestSize"),
    explicitMetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestThrottleRateAndTimeMs")
  )

  private def toMBeanName(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != "" }
    if (filteredTags.nonEmpty) {
      val tagsString = filteredTags.map { case (key, value) => "%s=%s".format(key, Sanitizer.jmxSanitize(value)) }.mkString(",")
      Some(tagsString)
    }
    else None
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def removeAllConsumerMetrics(clientId: String) {
    FetchRequestAndResponseStatsRegistry.removeConsumerFetchRequestAndResponseStats(clientId)
    ConsumerTopicStatsRegistry.removeConsumerTopicStat(clientId)
    ProducerRequestStatsRegistry.removeProducerRequestStats(clientId)
    removeAllMetricsInList(KafkaMetricsGroup.consumerMetricNameList, clientId)
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.10.0.0")
  def removeAllProducerMetrics(clientId: String) {
    ProducerRequestStatsRegistry.removeProducerRequestStats(clientId)
    ProducerTopicStatsRegistry.removeProducerTopicStats(clientId)
    ProducerStatsRegistry.removeProducerStats(clientId)
    removeAllMetricsInList(KafkaMetricsGroup.producerMetricNameList, clientId)
  }

  private def removeAllMetricsInList(metricNameList: immutable.List[String], clientId: String) {
    metricNameList.foreach(metric => {
      val pattern = (".*clientId=" + clientId + ".*").r
      kafkaMetricRegistry.getMetrics().asScala.keys.filter(metricNameList.contains(_))
        .foreach(name => pattern.findFirstIn(name) match {
          case Some(_) => {
            val beforeRemovalSize = kafkaMetricRegistry.getMetrics().size()
            kafkaMetricRegistry.remove(name)
            val afterRemovalSize = kafkaMetricRegistry.getMetrics().size()
            trace(s"Removing metric $name. Metrics registry size reduced from $beforeRemovalSize to $afterRemovalSize")
          }
          case _ =>
        })
    })
  }
}
