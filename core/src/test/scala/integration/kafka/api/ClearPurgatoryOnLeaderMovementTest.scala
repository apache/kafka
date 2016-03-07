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

package integration.kafka.api

import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.admin.{PreferredReplicaLeaderElectionCommand, ReassignPartitionsCommand}
import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.NotLeaderForPartitionException
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

/**
 * Test the case where produce requests in the purgatory should be cleared when the leader migrates to another broker.
 */
class ClearPurgatoryOnLeaderMovementTest extends KafkaServerTestHarness {

  val topic = "topic"
  val partition = 0
  def generateConfigs = {
    val overridingProps = new Properties()
    val numServers = 2
    overridingProps.put(KafkaConfig.NumPartitionsProp, 1.toString)
    // Make sure the fetch request will not return.
    overridingProps.put(KafkaConfig.ReplicaLagTimeMaxMsProp, "100000")
    overridingProps.put(KafkaConfig.ReplicaSocketTimeoutMsProp, "100000")
    overridingProps.put(KafkaConfig.ReplicaFetchMinBytesProp, "100000")
    overridingProps.put(KafkaConfig.ReplicaFetchWaitMaxMsProp, "100000")
    TestUtils.createBrokerConfigs(numServers, zkConnect, false, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile).map(KafkaConfig.fromProps(_, overridingProps))
  }

  @Test
  def testCleanProducerPurgatoryOnBecomeFollower() {
    TestUtils.createTopic(zkUtils, topic, 1, 2, servers, new Properties())
    TestUtils.waitUntilLeaderIsKnown(servers, topic, partition)
    val producer = TestUtils.createNewProducer(brokerList)

    // fetch the metadata.
    producer.partitionsFor(topic)
    val future = producer.send(new ProducerRecord(topic, "key".getBytes, "value".getBytes))
    // Wait 10 millisends to make sure the produce request has been sent. It is ugly but we don't have a way to
    // see if the producer has sent the produce request.
    Thread.sleep(10)
    assertFalse(future.isDone)
    // Trigger a leader migration
    if (TestUtils.isLeaderLocalOnBroker(topic, 0, servers(0)))
      moveLeader(oldLeader = 0, newLeader = 1)
    else
      moveLeader(oldLeader = 1, newLeader = 0)
    try {
      future.get()
      fail("Should throw ExecutionException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[NotLeaderForPartitionException])
    }

    def moveLeader(oldLeader: Int, newLeader: Int) {
      val tap = new TopicAndPartition(topic, partition)
      new ReassignPartitionsCommand(zkUtils, Map(tap -> Seq(newLeader, oldLeader))).reassignPartitions()
      TestUtils.waitUntilTrue(() => zkUtils.getPartitionsBeingReassigned().isEmpty, "Failed to finish partition assignment before timeout.")
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkUtils, Set(tap))
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils = zkUtils, topic = topic, partition = partition, newLeaderOpt = Some(newLeader))
      assertTrue(TestUtils.isLeaderLocalOnBroker(topic, partition, servers(newLeader)))
    }
  }

}
