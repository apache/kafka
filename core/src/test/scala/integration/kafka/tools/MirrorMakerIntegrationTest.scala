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
package kafka.tools

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.tools.MirrorMaker.{ConsumerWrapper, MirrorMakerProducer, NoRecordsException}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.junit.Test
import org.junit.Assert._

class MirrorMakerIntegrationTest extends KafkaServerTestHarness {

  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps(_, new Properties()))

  @Test(expected = classOf[TimeoutException])
  def testCommitOffsetsThrowTimeoutException(): Unit = {
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "1")
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer, new ByteArrayDeserializer)
    val mirrorMakerConsumer = new ConsumerWrapper(consumer, None, whitelistOpt = Some("any"))
    mirrorMakerConsumer.offsets.put(new TopicPartition("test", 0), 0L)
    mirrorMakerConsumer.commit()
  }

  @Test
  def testCommitOffsetsRemoveNonExistentTopics(): Unit = {
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "2000")
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer, new ByteArrayDeserializer)
    val mirrorMakerConsumer = new ConsumerWrapper(consumer, None, whitelistOpt = Some("any"))
    mirrorMakerConsumer.offsets.put(new TopicPartition("nonexistent-topic1", 0), 0L)
    mirrorMakerConsumer.offsets.put(new TopicPartition("nonexistent-topic2", 0), 0L)
    MirrorMaker.commitOffsets(mirrorMakerConsumer)
    assertTrue("Offsets for non-existent topics should be removed", mirrorMakerConsumer.offsets.isEmpty)
  }

  @Test
  def testCommaSeparatedRegex(): Unit = {
    val topic = "new-topic"
    val msg = "a test message"
    val brokerList = TestUtils.getBrokerListStrFromServers(servers)

    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    val producer = new MirrorMakerProducer(true, producerProps)
    MirrorMaker.producer = producer
    MirrorMaker.producer.send(new ProducerRecord(topic, msg.getBytes()))
    MirrorMaker.producer.close()

    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer, new ByteArrayDeserializer)

    val mirrorMakerConsumer = new ConsumerWrapper(consumer, None, whitelistOpt = Some("another_topic,new.*,foo"))
    mirrorMakerConsumer.init()
    try {
      TestUtils.waitUntilTrue(() => {
        try {
          val data = mirrorMakerConsumer.receive()
          data.topic == topic && new String(data.value) == msg
        } catch {
          // these exceptions are thrown if no records are returned within the timeout, so safe to ignore
          case _: NoRecordsException => false
        }
      }, "MirrorMaker consumer should read the expected message from the expected topic within the timeout")
    } finally consumer.close()
  }

}
