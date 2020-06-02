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

import kafka.server.KafkaConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import java.util.concurrent.ExecutionException

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.scalatest.Assertions.intercept

/**
 * Tests behavior of specifying auto topic creation configuration for the consumer and broker
 */
@RunWith(value = classOf[Parameterized])
class ProducerTopicCreationTest(brokerAutoTopicCreationEnable: JBoolean, producerAllowAutoCreateTopics: JBoolean) extends IntegrationTestHarness {
  override protected def brokerCount: Int = 1

  val topic = "topic"
  val producerClientId = "TestProducer"

  // configure server properties
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, brokerAutoTopicCreationEnable.toString)

  // configure client properties
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.producerConfig.setProperty(ProducerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, producerAllowAutoCreateTopics.toString)

  @Test
  def testAutoTopicCreation(): Unit = {
    val producer = createProducer()
    val record = new ProducerRecord(topic, 0, "key".getBytes, "value".getBytes)
    if (!(brokerAutoTopicCreationEnable && producerAllowAutoCreateTopics )) {
      intercept[ExecutionException] {
      producer.send(record).get
      }
    } else {
    producer.send(record).get
    // MetadataRequest is guaranteed to create the topic znode if creation was required
    val topicCreated = zkClient.getAllTopicsInCluster().contains(topic)
    assertTrue(topicCreated)
    }
  }
}

object ProducerTopicCreationTest {
  @Parameters(name = "brokerTopicCreation={0}, producerTopicCreation={1}")
  def parameters: java.util.Collection[Array[Object]] = {
    val data = new java.util.ArrayList[Array[Object]]()
    for (brokerAutoTopicCreationEnable <- Array(JBoolean.TRUE, JBoolean.FALSE))
      for (producerAutoCreateTopicsPolicy <- Array(JBoolean.TRUE, JBoolean.FALSE))
        data.add(Array(brokerAutoTopicCreationEnable, producerAutoCreateTopicsPolicy))
    data
  }
}
