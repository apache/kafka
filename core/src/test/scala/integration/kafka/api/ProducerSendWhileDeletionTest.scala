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

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.Properties
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets


class ProducerSendWhileDeletionTest extends BaseProducerSendTest {


    override def generateConfigs = {
        val overridingProps = new Properties()
        val numServers = 2
        overridingProps.put(KafkaConfig.NumPartitionsProp, 2.toString)
        overridingProps.put(KafkaConfig.DefaultReplicationFactorProp, 2.toString)
        overridingProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
        TestUtils.createBrokerConfigs(numServers, zkConnect, false, interBrokerSecurityProtocol = Some(securityProtocol),
                trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties).map(KafkaConfig.fromProps(_, overridingProps))
    }

    /**
     * Tests that Producer gets self-recovered when a topic is deleted mid-way of produce.
     *
     * Producer will attempt to send messages to the partition specified in each record, and should
     * succeed as long as the partition is included in the metadata.
     */
    @Test
    def testSendWithTopicDeletionMidWay(): Unit = {
        val numRecords = 10

        // create topic with leader as 0 for the 2 partitions.
        createTopic(topic, Map(0 -> Seq(0, 1), 1 -> Seq(0, 1)))

        val reassignment = Map(
            new TopicPartition(topic, 0) -> Seq(1, 0),
            new TopicPartition(topic, 1) -> Seq(1, 0)
        )

        // Change leader to 1 for both the partitions to increase leader Epoch from 0 -> 1
        zkClient.createPartitionReassignment(reassignment)
        TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
            "failed to remove reassign partitions path after completion")

        val producer = createProducer(brokerList, maxBlockMs = 5 * 1000L, deliveryTimeoutMs = 20 * 1000)

        (1 to numRecords).map { i =>
            val resp = producer.send(new ProducerRecord(topic, null, ("value" + i).getBytes(StandardCharsets.UTF_8))).get
            assertEquals(topic, resp.topic())
        }

        // start topic deletion
        adminZkClient.deleteTopic(topic)

        // Verify that the topic is deleted when no metadata request comes in
        TestUtils.verifyTopicDeletion(zkClient, topic, 2, servers)

        // Producer should be able to send messages even after topic gets deleted and auto-created
        assertEquals(topic, producer.send(new ProducerRecord(topic, null, ("value").getBytes(StandardCharsets.UTF_8))).get.topic())
    }


}
