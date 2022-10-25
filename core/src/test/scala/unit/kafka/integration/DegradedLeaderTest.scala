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

package unit.kafka.integration

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

class DegradedLeaderTest extends ZooKeeperTestHarness {
  @Test
  def testLeadershipTransferByDegradedLeader(): Unit = {
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(5, zkConnect, false)
      .map(props => {
        if (props.get(KafkaConfig.BrokerIdProp).equals("0")) {
          // let the leader drop Fetch requests from the followers, which will cause the partition to be UnderMinISR
          props.setProperty(KafkaConfig.LiDropFetchFollowerEnableProp, "true")
        }
        props.put(KafkaConfig.ReplicaLagTimeMaxMsProp, "10000")
        props
      })
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure broker 4 becomes the controller
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    // val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    assertTrue(controllerId == 4)

    // create the topic with min ISR of 2
    val topic = "test"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2, 3))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    /**
     * An out-of-sync replica is defined to be one whose log end offset is different from that of the leader,
     * and whose lastCaughtUpTimeMs has been too long in the past.
     * When no messages have been produced, the log end offsets of all replicas have the same value, i.e. 0.
     * Thus there will be no UnderMinISR partitions.
     * The UnderMinISR partitions will only happen after some messages are produced.
     */

    val adminClient = TestUtils.createAdminClient(servers)
    val initialLeader = 0
    assertTrue(getLeader(adminClient, tp) == initialLeader)

    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers))
    produceRecord(producer, topic, partition)

    TestUtils.waitUntilTrue(() => {
      // wait until the leadership has switch to a different broker
      val newLeader = getLeader(adminClient, tp)
      newLeader != initialLeader
    }, "the leadership hasn't switched")

    // make sure after the leadership switch, the producing still works
    produceRecord(producer, topic, partition)

    adminClient.close()
    producer.close()
    servers.foreach(_.shutdown())
  }

  private def getLeader(adminClient: Admin, tp: TopicPartition): Int = {
    val topicDescMap = adminClient.describeTopics(Collections.singleton(tp.topic())).all().get()
    topicDescMap.get(tp.topic()).partitions().get(tp.partition()).leader().id()
  }

  private def produceRecord(producer: Producer[Array[Byte], Array[Byte]], topic: String, partition: Int) = {
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8))
    producer.send(record).get()
  }
}
