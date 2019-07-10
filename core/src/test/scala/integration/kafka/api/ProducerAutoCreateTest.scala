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
  
package integration.kafka.api

import java.util.Properties

import kafka.api.BaseProducerSendTest
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.junit.Assert.assertEquals
import org.junit.Test

class ProducerAutoCreateTest extends BaseProducerSendTest  {
  override def generateConfigs = {
    val overridingProps = new Properties()
    val numServers = 2
    overridingProps.put(KafkaConfig.NumPartitionsProp, 4.toString)
    overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    TestUtils.createBrokerConfigs(numServers, zkConnect, false, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties).map(KafkaConfig.fromProps(_, overridingProps))
  }

  /**
   * testAutoCreateTopicProducer
   *
   * The topic should be created upon sending the first message
   */
  @Test
  def testAutoCreateTopicProducer() {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "true")
    val producer = registerProducer(new KafkaProducer(producerProps))
    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, 3, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record).get.offset)

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)

    } finally {
      producer.close()
    }
  }

  /**
   * testAutoCreateTopicProducer
   *
   * The topic should be created upon sending the first message
   */
  @Test
  def testAutoCreateTopicProducerPartitions() {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "true")
    producerProps.put(ProducerConfig.AUTO_CREATE_NUM_PARTITIONS_CONFIG, "10")
    producerProps.put(ProducerConfig.AUTO_CREATE_REPLICATION_FACTOR_CONFIG, "2")
    val producer = registerProducer(new KafkaProducer(producerProps))
    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, 9, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record).get.offset)

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)

    } finally {
      producer.close()
    }
  }  
}
