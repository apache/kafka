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
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets


class ProducerSendWhileDeletionTest extends IntegrationTestHarness {
    val producerCount: Int = 1
    val brokerCount: Int = 2

    serverConfig.put(KafkaConfig.NumPartitionsProp, 2.toString)
    serverConfig.put(KafkaConfig.DefaultReplicationFactorProp, 2.toString)
    serverConfig.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)

    producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L.toString)
    producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000.toString)
    producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000.toString)

    /**
     * Tests that Producer gets self-recovered when a topic is deleted mid-way of produce.
     *
     * Producer will attempt to send messages to the partition specified in each record, and should
     * succeed as long as the partition is included in the metadata.
     */
    @Test
    def testSendWithTopicDeletionMidWay(): Unit = {
        val numRecords = 10
        val topic = "topic"

        // Create topic with leader as 0 for the 2 partitions.
        createTopicWithAssignment(topic, Map(0 -> Seq(0, 1), 1 -> Seq(0, 1)))

        val reassignment = Map(
            new TopicPartition(topic, 0) -> Seq(1, 0),
            new TopicPartition(topic, 1) -> Seq(1, 0)
        )

        // Change leader to 1 for both the partitions to increase leader epoch from 0 -> 1
        zkClient.createPartitionReassignment(reassignment)
        TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
            "failed to remove reassign partitions path after completion")

        val producer = createProducer()

        (1 to numRecords).foreach { i =>
            val resp = producer.send(new ProducerRecord(topic, null, ("value" + i).getBytes(StandardCharsets.UTF_8))).get
            assertEquals(topic, resp.topic())
        }

        // Start topic deletion
        adminZkClient.deleteTopic(topic)

        // Verify that the topic is deleted when no metadata request comes in
        TestUtils.verifyTopicDeletion(zkClient, topic, 2, servers)

        // Producer should be able to send messages even after topic gets deleted and auto-created
        assertEquals(topic, producer.send(new ProducerRecord(topic, null, "value".getBytes(StandardCharsets.UTF_8))).get.topic())
    }

}
