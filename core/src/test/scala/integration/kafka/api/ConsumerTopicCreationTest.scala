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

package kafka.api

import java.lang.{Boolean => JBoolean}
import java.time.Duration
import java.util
import java.util.Collections

import kafka.api
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

/**
 * Tests behavior of specifying auto topic creation configuration for the consumer and broker
 */
class ConsumerTopicCreationTest {

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testAutoTopicCreation(brokerAutoTopicCreationEnable: JBoolean, consumerAllowAutoCreateTopics: JBoolean): Unit = {
    val testCase = new ConsumerTopicCreationTest.TestCase(brokerAutoTopicCreationEnable, consumerAllowAutoCreateTopics)
    testCase.setUp()
    try testCase.test() finally testCase.tearDown()
  }

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testAutoTopicCreationWithForwarding(brokerAutoTopicCreationEnable: JBoolean, consumerAllowAutoCreateTopics: JBoolean): Unit = {
    val testCase = new api.ConsumerTopicCreationTest.TestCaseWithForwarding(brokerAutoTopicCreationEnable, consumerAllowAutoCreateTopics)
    testCase.setUp()
    try testCase.test() finally testCase.tearDown()
  }
}

object ConsumerTopicCreationTest {

  private class TestCaseWithForwarding(brokerAutoTopicCreationEnable: JBoolean, consumerAllowAutoCreateTopics: JBoolean)
    extends TestCase(brokerAutoTopicCreationEnable, consumerAllowAutoCreateTopics) {

    override protected def brokerCount: Int = 3

    override def enableForwarding: Boolean = true
  }

  private class TestCase(brokerAutoTopicCreationEnable: JBoolean, consumerAllowAutoCreateTopics: JBoolean) extends IntegrationTestHarness {
    private val topic_1 = "topic-1"
    private val topic_2 = "topic-2"
    private val producerClientId = "ConsumerTestProducer"
    private val consumerClientId = "ConsumerTestConsumer"

    // configure server properties
    this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
    this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, brokerAutoTopicCreationEnable.toString)

    // configure client properties
    this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
    this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")
    this.consumerConfig.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, consumerAllowAutoCreateTopics.toString)
    override protected def brokerCount: Int = 1


    def test(): Unit = {
      val consumer = createConsumer()
      val producer = createProducer()
      val adminClient = createAdminClient()
      val record = new ProducerRecord(topic_1, 0, "key".getBytes, "value".getBytes)

      // create `topic_1` and produce a record to it
      adminClient.createTopics(Collections.singleton(new NewTopic(topic_1, 1, 1.toShort))).all.get
      producer.send(record).get

      consumer.subscribe(util.Arrays.asList(topic_1, topic_2))

      // Wait until the produced record was consumed. This guarantees that metadata request for `topic_2` was sent to the
      // broker.
      TestUtils.waitUntilTrue(() => {
        consumer.poll(Duration.ofMillis(100)).count > 0
      }, "Timed out waiting to consume")

      // MetadataRequest is guaranteed to create the topic znode if creation was required
      val topicCreated = zkClient.getAllTopicsInCluster().contains(topic_2)
      if (brokerAutoTopicCreationEnable && consumerAllowAutoCreateTopics)
        assertTrue(topicCreated)
      else
        assertFalse(topicCreated)
    }
  }

  def parameters: java.util.stream.Stream[Arguments] = {
    val data = new java.util.ArrayList[Arguments]()
    for (brokerAutoTopicCreationEnable <- Array(JBoolean.TRUE, JBoolean.FALSE))
      for (consumerAutoCreateTopicsPolicy <- Array(JBoolean.TRUE, JBoolean.FALSE))
        data.add(Arguments.of(brokerAutoTopicCreationEnable, consumerAutoCreateTopicsPolicy))
    data.stream()
  }
}
