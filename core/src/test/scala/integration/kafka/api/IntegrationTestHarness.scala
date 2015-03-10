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

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import kafka.utils.TestUtils
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.server.{OffsetManager, KafkaConfig}
import kafka.integration.KafkaServerTestHarness
import scala.collection.mutable.Buffer

/**
 * A helper class for writing integration tests that involve producers, consumers, and servers
 */
trait IntegrationTestHarness extends KafkaServerTestHarness {

  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties
  override lazy val configs = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount)
    cfgs.map(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }
  
  var consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  var producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()
  
  override def setUp() {
    super.setUp()
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapUrl)
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapUrl)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    for(i <- 0 until producerCount)
      producers += new KafkaProducer(producerConfig)
    for(i <- 0 until consumerCount)
      consumers += new KafkaConsumer(consumerConfig)

    // create the consumer offset topic
    TestUtils.createTopic(zkClient, OffsetManager.OffsetsTopicName,
      serverConfig.getProperty("offsets.topic.num.partitions").toInt,
      serverConfig.getProperty("offsets.topic.replication.factor").toInt,
      servers,
      servers(0).offsetManager.offsetsTopicConfig)
  }
  
  override def tearDown() {
    producers.map(_.close())
    consumers.map(_.close())
    super.tearDown()
  }

}
