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
package kafka.api

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Properties
import java.util.concurrent.TimeUnit

class ProducerRebootstrapTest extends AbstractConsumerTest {
  override def brokerCount: Int = 2

  def server0: KafkaServer = serverForId(0).get
  def server1: KafkaServer = serverForId(1).get

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, brokerCount.toString)
    overridingProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")

    // In this test, fixed ports are necessary, because brokers must have the
    // same port after the restart.
    FixedPortTestUtils.createBrokerConfigs(brokerCount, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @Test
  def testRebootstrap(): Unit = {
    server1.shutdown()
    server1.awaitShutdown()

    val producerConfigOverrides = new Properties()
    producerConfigOverrides.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap");
    val producer = createProducer(configOverrides = producerConfigOverrides)

    // Only the server 0 is available for the producer during the bootstrap.
    producer.send(new ProducerRecord(topic, part, "key 0".getBytes, "value 0".getBytes))
      .get(1, TimeUnit.MINUTES)

    server0.shutdown()
    server0.awaitShutdown()
    server1.startup()

    // The server 0, originally cached during the bootstrap, is offline.
    // However, the server 1 from the bootstrap list is online.
    // Should be able to produce records.
    val recordMetadata1 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes))
      .get(1, TimeUnit.MINUTES)
    assertEquals(0, recordMetadata1.offset())

    server1.shutdown()
    server1.awaitShutdown()
    server0.startup()

    // The same situation, but the server 1 has gone and server 0 is back.
    val recordMetadata2 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes))
      .get(1, TimeUnit.MINUTES)
    assertEquals(1, recordMetadata2.offset())
  }
}
