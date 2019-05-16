/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */

package integration.kafka.api

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import kafka.api.{AbstractConsumerTest, FixedPortTestUtils}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.junit.Test

/**
  * This test checks if a consumer and a producer are able to continue working
  * if no broker they cached during the bootstrap is available,
  * but some brokers from the initial bootstrap list are available.
  * <p>
  * This situation is possible when a consumer or producer was bootstrapped
  * when the cluster was partially available and later the nodes that were
  * initially available went down and some other nodes from the bootstrap list
  * came online.
  */
class RebootstrapTest extends AbstractConsumerTest with Logging {

  override val brokerCount: Int = 2

  def server0: KafkaServer = serverForId(0).get
  def server1: KafkaServer = serverForId(1).get

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(KafkaConfig.OffsetsTopicReplicationFactorProp, brokerCount.toString)
    overridingProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")

    // In this test, fixed ports are necessary, because brokers must have the
    // same port after the restart.
    FixedPortTestUtils.createBrokerConfigs(brokerCount, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @Test
  def testConsumer(): Unit = {
    sendRecords(10, 0)

    server1.shutdown()
    server1.awaitShutdown()

    // Only server 0 is available for the consumer during the bootstrap.
    val consumer = createConsumer()
    consumer.assign(Collections.singleton(tp))

    consumeAndVerifyRecords(consumer, 10, 0)

    server0.shutdown()
    server0.awaitShutdown()

    server1.startup()

    sendRecords(10, 10)

    // Server 0, originally cached during the bootstrap, is offline.
    // However, server 1 from the bootstrap list is online.
    // Should be able to consume records.
    consumeAndVerifyRecords(consumer, 10, 10, startingKeyAndValueIndex = 10, startingTimestamp = 10)
  }

  private def sendRecords(numRecords: Int, from: Int): Unit = {
    val producer: KafkaProducer[Array[Byte], Array[Byte]] = createProducer()
    (from until (numRecords + from)).foreach { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()
    producer.close()
  }

  @Test
  def testProducer(): Unit = {
    server1.shutdown()
    server1.awaitShutdown()

    // Only server 0 is available for the producer during the bootstrap.
    val producer = createProducer()
    producer.send(new ProducerRecord(topic, part, "key 0".getBytes, "value 0".getBytes))
      .get(1, TimeUnit.MINUTES)

    server0.shutdown()
    server0.awaitShutdown()

    server1.startup()

    // Server 0, originally cached during the bootstrap, is offline.
    // However, server 1 from the bootstrap list is online.
    // Should be able to produce records.
    producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes))
      .get(1, TimeUnit.MINUTES)
  }
}
