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

import kafka.consumer.ConsumerTimeoutException
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.tools.MirrorMaker.{MirrorMakerNewConsumer, MirrorMakerProducer}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.junit.Test

class MirrorMakerIntegrationTest extends KafkaServerTestHarness {

  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps(_, new Properties()))

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

    val mirrorMakerConsumer = new MirrorMakerNewConsumer(consumer, None, whitelistOpt = Some("another_topic,new.*,foo"))
    mirrorMakerConsumer.init()
    try {
      TestUtils.waitUntilTrue(() => {
        try {
          val data = mirrorMakerConsumer.receive()
          data.topic == topic && new String(data.value) == msg
        } catch {
          // this exception is thrown if no record is returned within a short timeout, so safe to ignore
          case _: ConsumerTimeoutException => false
        }
      }, "MirrorMaker consumer should read the expected message from the expected topic within the timeout")
    } finally consumer.close()
  }

}
