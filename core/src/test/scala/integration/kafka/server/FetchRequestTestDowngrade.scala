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

package kafka.server

import java.time.Duration
import java.util.Arrays.asList

import kafka.utils.TestUtils
import kafka.zk.ZkVersion
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_7_IV0, IBP_3_1_IV0}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.{Map, Seq}

class FetchRequestTestDowngrade extends BaseRequestTest {

    override def brokerCount: Int = 2
    override def generateConfigs: Seq[KafkaConfig] = {
        // Controller should start with newer IBP and downgrade to the older one.
        Seq(
            createConfig(0, IBP_3_1_IV0),
            createConfig(1, IBP_2_7_IV0)
        )
    }

    @Test
    def testTopicIdIsRemovedFromFetcherWhenControllerDowngrades(): Unit = {
        val tp = new TopicPartition("topic", 0)
        val producer = createProducer()
        val consumer = createConsumer()

        ensureControllerIn(Seq(0))
        assertEquals(0, controllerSocketServer.config.brokerId)
        val partitionLeaders = createTopicWithAssignment(tp.topic, Map(tp.partition -> Seq(1, 0)))
        TestUtils.waitForAllPartitionsMetadata(servers, tp.topic, 1)
        ensureControllerIn(Seq(1))
        assertEquals(1, controllerSocketServer.config.brokerId)

        assertEquals(1, partitionLeaders(0))

        val record1 = new ProducerRecord(tp.topic, tp.partition, null, "key".getBytes, "value".getBytes)
        producer.send(record1)

        consumer.assign(asList(tp))
        val count = consumer.poll(Duration.ofMillis(5000)).count()
        assertEquals(1, count)
    }

    private def ensureControllerIn(brokerIds: Seq[Int]): Unit = {
        while (!brokerIds.contains(controllerSocketServer.config.brokerId)) {
            zkClient.deleteController(ZkVersion.MatchAnyVersion)
            TestUtils.waitUntilControllerElected(zkClient)
        }
    }

    private def createConfig(nodeId: Int, interBrokerVersion: MetadataVersion): KafkaConfig = {
        val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
        props.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, interBrokerVersion.version)
        KafkaConfig.fromProps(props)
    }

}
