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

import kafka.integration.KafkaServerTestHarness
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test

import java.util
import java.util.Properties

class RecommendedLeaderElectionTest extends KafkaServerTestHarness {
  val numNodes = 2
  val overridingProps = new Properties
  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  @Test
  def testRecommendedLeaderElection(): Unit = {
    val topic = "test"
    val tp = new TopicPartition(topic, 0)
    createTopic(topic, 1, 2)

    val currentLeader = zkClient.getTopicPartitionState(tp).get.leaderAndIsr.leader
    val currentFollower = if (currentLeader == 0) {
      1
    } else {
      0
    }
    val newLeader = currentFollower

    // elect the recommended leader for the partition
    val adminClient = createAdminClient()
    val partitionWithRecommendedLeaders = new util.HashMap[TopicPartition, Integer]()
    partitionWithRecommendedLeaders.put(tp, newLeader)
    adminClient.electRecommendedLeaders(partitionWithRecommendedLeaders).all().get()
    adminClient.close()

    // wait until the leader has changed to the recommended one
    TestUtils.waitUntilTrue(() => {
      zkClient.getTopicPartitionState(tp).get.leaderAndIsr.leader == newLeader
    }, s"The leader cannot be changed to the recommended one $newLeader")
  }

  @Test
  def testRecommendedLeaderElectionWithoutLeaderInRequest(): Unit = {
    val topic = "test"
    createTopic(topic, 1, 2)

    val adminClient = createAdminClient()
    val partitionWithRecommendedLeaders = new util.HashMap[TopicPartition, Integer]()
    // elect with empty map should result in no exceptions
    adminClient.electRecommendedLeaders(partitionWithRecommendedLeaders).all().get()
    adminClient.close()
  }

  def createAdminClient(props: Properties = new Properties): Admin = {
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    Admin.create(props)
  }
}
