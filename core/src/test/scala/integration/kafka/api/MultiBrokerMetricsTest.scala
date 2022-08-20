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

import kafka.server.BrokerTopicStats
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

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
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "1")
    val producer = new KafkaProducer(producerConfig, new ByteArraySerializer, new ByteArraySerializer)
    TestUtils.sendRecords(producer, numRecords, recordSize, tp)
    producer.close()

    TestUtils.verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=${BrokerTopicStats.ProduceRequestsWithInvalidAcksPerSec},topic=$topic")
    TestUtils.verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=${BrokerTopicStats.ProduceRequestsWithInvalidAcksPerSec}")
  }
}
