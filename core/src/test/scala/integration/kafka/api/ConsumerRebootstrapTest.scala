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
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Test

import java.util.{Collections, Properties}

class ConsumerRebootstrapTest extends AbstractConsumerTest {
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
    sendRecords(10, 0)

    TestUtils.waitUntilTrue(
      () => server0.logManager.logsByTopic(tp.topic()).head.logEndOffset == server1.logManager.logsByTopic(tp.topic()).head.logEndOffset,
      "Timeout waiting for records to be replicated"
    )

    server1.shutdown()
    server1.awaitShutdown()

    val consumerConfigOverrides = new Properties()
    consumerConfigOverrides.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap");
    val consumer = createConsumer(configOverrides = consumerConfigOverrides)

    // Only the server 0 is available for the consumer during the bootstrap.
    consumer.assign(Collections.singleton(tp))

    consumeAndVerifyRecords(consumer, 10, 0)

    // Bring back the server 1 and shut down 0.
    server1.startup()

    TestUtils.waitUntilTrue(
      () => server0.logManager.logsByTopic(tp.topic()).head.logEndOffset == server1.logManager.logsByTopic(tp.topic()).head.logEndOffset,
      "Timeout waiting for records to be replicated"
    )

    server0.shutdown()
    server0.awaitShutdown()
    sendRecords(10, 10)

    // The server 0, originally cached during the bootstrap, is offline.
    // However, the server 1 from the bootstrap list is online.
    // Should be able to consume records.
    consumeAndVerifyRecords(consumer, 10, 10, startingKeyAndValueIndex = 10, startingTimestamp = 10)

    // Bring back the server 0 and shut down 1.
    server0.startup()

    TestUtils.waitUntilTrue(
      () => server0.logManager.logsByTopic(tp.topic()).head.logEndOffset == server1.logManager.logsByTopic(tp.topic()).head.logEndOffset,
      "Timeout waiting for records to be replicated"
    )

    server1.shutdown()
    server1.awaitShutdown()
    sendRecords(10, 20)

    // The same situation, but the server 1 has gone and server 0 is back.
    consumeAndVerifyRecords(consumer, 10, 20, startingKeyAndValueIndex = 20, startingTimestamp = 20)
  }

  private def sendRecords(numRecords: Int, from: Int): Unit = {
    val producer: KafkaProducer[Array[Byte], Array[Byte]] = createProducer()
    (from until (numRecords + from)).foreach { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
    }
    producer.flush()
    producer.close()
  }
}
