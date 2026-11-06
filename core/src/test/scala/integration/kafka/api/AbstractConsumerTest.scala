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

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import kafka.server.BaseRequestTest
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.ServerConfigs

/**
 * Extension point for consumer integration tests.
 */
abstract class AbstractConsumerTest extends BaseRequestTest {

  override def brokerCount: Int = 3

  val topic = "topic"
  val group = "my-test"
  val producerClientId = "ConsumerTestProducer"
  val consumerClientId = "ConsumerTestConsumer"
  val groupMaxSessionTimeoutMs = 60000L

  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "6000")


  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false") // speed up shutdown
    properties.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "3") // don't want to lose offset
    properties.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
    properties.setProperty(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "100") // set small enough session timeout
    properties.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, groupMaxSessionTimeoutMs.toString)
    properties.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "10")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    // create the test topic with all the brokers as replicas
    createTopic(topic, 2, brokerCount, adminClientConfig = this.adminClientConfig)
  }

}
