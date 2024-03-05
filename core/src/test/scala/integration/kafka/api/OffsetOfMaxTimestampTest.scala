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

import kafka.api.IntegrationTestHarness
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, NewTopic, OffsetSpec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.{Collections, Properties}

class OffsetOfMaxTimestampTest extends IntegrationTestHarness {
  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  override def brokerCount: Int = 1

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testWithNoCompression(quorum: String): Unit = {
    test(false)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testWithCompression(quorum: String): Unit = {
    test(true)
  }

  private def test(useCompression: Boolean): Unit = {
    val topicName: String = "OffsetOfMaxTimestampTest-" + System.currentTimeMillis()

    val admin: Admin = Admin.create(adminClientConfig)
    try {
      admin.createTopics(Collections.singletonList(new NewTopic(topicName, 1, 1.toShort)
        .configs(Collections.singletonMap(TopicConfig.COMPRESSION_TYPE_CONFIG, if (useCompression) "gzip" else "none"))))
      val props: Properties = new Properties
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, if (useCompression) "gzip" else "none")
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      try {
        val time: Long = 10000
        producer.send(new ProducerRecord[String, String](topicName, 0, time + 100, null, "val20"))
        producer.send(new ProducerRecord[String, String](topicName, 0, time + 400, null, "val15"))
        producer.send(new ProducerRecord[String, String](topicName, 0, time + 250, null, "val15"))
        producer.flush()
      } finally producer.close()

      val result = admin.listOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), OffsetSpec.maxTimestamp)).all.get
      assertEquals(1, result.size)
      assertEquals(1, result.get(new TopicPartition(topicName, 0)).offset)

    } finally admin.close()
  }
}