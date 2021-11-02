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

package integration.kafka.server

import java.time.Duration
import java.util.Arrays.asList

import kafka.api.{ApiVersion, KAFKA_2_7_IV0, KAFKA_3_1_IV0}
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import kafka.zk.ZkVersion
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.{Map, Seq}

class FetchRequestTestDowngrade extends BaseRequestTest {

    override def brokerCount: Int = 3
    override def generateConfigs: Seq[KafkaConfig] = {
        // Brokers should start with newer IBP and downgrade to the older one.
        Seq(
                createConfig(0, KAFKA_3_1_IV0),
                createConfig(1, KAFKA_3_1_IV0),
                createConfig(2, KAFKA_2_7_IV0)
        )
    }

    @Test
    def testTopicIdsInFetcherOldController(): Unit = {
        val topic = "topic"
        val producer = createProducer()
        val consumer = createConsumer()

        ensureControllerIn(Seq(0))
        assertEquals(0, controllerSocketServer.config.brokerId)
        val partitionLeaders = createTopic(topic,  Map(0 -> Seq(1, 0, 2), 1 -> Seq(0, 2, 1)))
        TestUtils.waitForAllPartitionsMetadata(servers, topic, 2)
        ensureControllerIn(Seq(2))
        assertEquals(2, controllerSocketServer.config.brokerId)

        assertEquals(1, partitionLeaders(0))
        assertEquals(0, partitionLeaders(1))

        val record1 = new ProducerRecord(topic, 0, null, "key".getBytes, "value".getBytes)
        val record2 = new ProducerRecord(topic, 1, null, "key".getBytes, "value".getBytes)
        producer.send(record1)
        producer.send(record2)

        consumer.assign(asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
        val count = consumer.poll(Duration.ofMillis(5000)).count() + consumer.poll(Duration.ofMillis(5000)).count()
        assertEquals(2, count)
    }

    private def ensureControllerIn(brokerIds: Seq[Int]): Unit = {
        while (!brokerIds.contains(controllerSocketServer.config.brokerId)) {
            zkClient.deleteController(ZkVersion.MatchAnyVersion)
            TestUtils.waitUntilControllerElected(zkClient)
        }
    }

    private def createConfig(nodeId: Int, interBrokerVersion: ApiVersion): KafkaConfig = {
        val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
        props.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerVersion.version)
        KafkaConfig.fromProps(props)
    }

} 