/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import com.yammer.metrics.core.{Gauge, Histogram, Meter}
import kafka.server.BrokerTopicStats
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.util.Properties
import scala.jdk.CollectionConverters._

class MultiBrokerMetricsTest extends IntegrationTestHarness {
  override val brokerCount = 4

  @Test
  def testProduceRequestsWithInvalidAcks(): Unit = {
    val topic = "Topic1"
    val props = new Properties
    props.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    createTopic(topic, numPartitions = 1, replicationFactor = 4, props)
    val tp = new TopicPartition(topic, 0)

    val numRecords = 10
    val recordSize = 100000
    val producerConfig = new Properties
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "1")
    val producer = new KafkaProducer(producerConfig, new ByteArraySerializer, new ByteArraySerializer)
    sendRecords(producer, numRecords, recordSize, tp)
    producer.close()

    verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=${BrokerTopicStats.ProduceRequestsWithInvalidAcksPerSec},topic=$topic")
    verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=${BrokerTopicStats.ProduceRequestsWithInvalidAcksPerSec}")
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
      recordSize: Int, tp: TopicPartition): Unit = {
    val bytes = new Array[Byte](recordSize)
    (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, bytes))
    }
    producer.flush()
  }

  private def verifyYammerMetricRecorded(name: String, verify: Double => Boolean = d => d > 0): Double = {
    val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
    val (_, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(throw new AssertionError(s"Unable to find broker metric $name"))
    val metricValue = metric match {
      case m: Meter => m.count.toDouble
      case m: Histogram => m.max
      case m: Gauge[_] => m.value.asInstanceOf[Double]
      case m => throw new AssertionError(s"Unexpected broker metric of class ${m.getClass}")
    }
    assertTrue(verify(metricValue), s"Broker metric not recorded correctly for $name value $metricValue")
    metricValue
  }
}
