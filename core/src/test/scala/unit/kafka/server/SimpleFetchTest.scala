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

import kafka.api._
import kafka.utils._
import kafka.cluster.Replica
import kafka.common.TopicAndPartition
import kafka.log.Log
import kafka.message.{ByteBufferMessageSet, Message, MessageSet}
import kafka.server.QuotaFactory.UnboundedQuota
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{MockTime => JMockTime}

import org.junit.{Test, After, Before}

import java.util.{Properties}
import java.util.concurrent.atomic.AtomicBoolean
import collection.JavaConversions._

import org.easymock.EasyMock
import org.junit.Assert._

class SimpleFetchTest {

  val replicaLagTimeMaxMs = 100L
  val replicaFetchWaitMaxMs = 100
  val replicaLagMaxMessages = 10L

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.ReplicaLagTimeMaxMsProp, replicaLagTimeMaxMs.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchWaitMaxMsProp, replicaFetchWaitMaxMs.toString)

  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  // set the replica manager with the partition
  val time = new MockTime
  val jTime = new JMockTime
  val metrics = new Metrics
  val leaderLEO = 20L
  val followerLEO = 15L
  val partitionHW = 5

  val fetchSize = 100
  val messagesToHW = new Message("messageToHW".getBytes())
  val messagesToLEO = new Message("messageToLEO".getBytes())

  val topic = "test-topic"
  val partitionId = 0
  val topicAndPartition = TopicAndPartition(topic, partitionId)

  val fetchInfo = Seq(topicAndPartition -> PartitionFetchInfo(0, fetchSize))

  var replicaManager: ReplicaManager = null

  @Before
  def setUp() {
    // create nice mock since we don't particularly care about zkclient calls
    val zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
    EasyMock.replay(zkUtils)

    // create nice mock since we don't particularly care about scheduler calls
    val scheduler = EasyMock.createNiceMock(classOf[KafkaScheduler])
    EasyMock.replay(scheduler)

    // create the log which takes read with either HW max offset or none max offset
    val log = EasyMock.createMock(classOf[Log])
    EasyMock.expect(log.logEndOffset).andReturn(leaderLEO).anyTimes()
    EasyMock.expect(log.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(leaderLEO)).anyTimes()
    EasyMock.expect(log.read(0, fetchSize, Some(partitionHW), true)).andReturn(
      new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        new ByteBufferMessageSet(messagesToHW)
      )).anyTimes()
    EasyMock.expect(log.read(0, fetchSize, None, true)).andReturn(
      new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        new ByteBufferMessageSet(messagesToLEO)
      )).anyTimes()
    EasyMock.replay(log)

    // create the log manager that is aware of this mock log
    val logManager = EasyMock.createMock(classOf[kafka.log.LogManager])
    EasyMock.expect(logManager.getLog(topicAndPartition)).andReturn(Some(log)).anyTimes()
    EasyMock.replay(logManager)

    // create the replica manager
    replicaManager = new ReplicaManager(configs.head, metrics, time, jTime, zkUtils, scheduler, logManager,
      new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time).follower)

    // add the partition with two replicas, both in ISR
    val partition = replicaManager.getOrCreatePartition(topic, partitionId)

    // create the leader replica with the local log
    val leaderReplica = new Replica(configs.head.brokerId, partition, time, 0, Some(log))
    leaderReplica.highWatermark = new LogOffsetMetadata(partitionHW)
    partition.leaderReplicaIdOpt = Some(leaderReplica.brokerId)

    // create the follower replica with defined log end offset
    val followerReplica= new Replica(configs(1).brokerId, partition, time)
    val leo = new LogOffsetMetadata(followerLEO, 0L, followerLEO.toInt)
    followerReplica.updateLogReadResult(new LogReadResult(FetchDataInfo(leo, MessageSet.Empty), -1L, -1, true))

    // add both of them to ISR
    val allReplicas = List(leaderReplica, followerReplica)
    allReplicas.foreach(partition.addReplicaIfNotExists(_))
    partition.inSyncReplicas = allReplicas.toSet
  }

  @After
  def tearDown() {
    replicaManager.shutdown(false)
    metrics.close()
  }

  /**
   * The scenario for this test is that there is one topic that has one partition
   * with one leader replica on broker "0" and one follower replica on broker "1"
   * inside the replica manager's metadata.
   *
   * The leader replica on "0" has HW of "5" and LEO of "20".  The follower on
   * broker "1" has a local replica with a HW matching the leader's ("5") and
   * LEO of "15", meaning it's not in-sync but is still in ISR (hasn't yet expired from ISR).
   *
   * When a fetch operation with read committed data turned on is received, the replica manager
   * should only return data up to the HW of the partition; when a fetch operation with read
   * committed data turned off is received, the replica manager could return data up to the LEO
   * of the local leader replica's log.
   *
   * This test also verifies counts of fetch requests recorded by the ReplicaManager
   */
  @Test
  def testReadFromLog() {
    val initialTopicCount = BrokerTopicStats.getBrokerTopicStats(topic).totalFetchRequestRate.count()
    val initialAllTopicsCount = BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.count()

    assertEquals("Reading committed data should return messages only up to high watermark", messagesToHW,
      replicaManager.readFromLocalLog(true, true, Int.MaxValue, false, fetchInfo, UnboundedQuota).find(_._1 == topicAndPartition).get._2.info.messageSet.head.message)
    assertEquals("Reading any data can return messages up to the end of the log", messagesToLEO,
      replicaManager.readFromLocalLog(true, false, Int.MaxValue, false, fetchInfo, UnboundedQuota).find(_._1 == topicAndPartition).get._2.info.messageSet.head.message)

    assertEquals("Counts should increment after fetch", initialTopicCount+2, BrokerTopicStats.getBrokerTopicStats(topic).totalFetchRequestRate.count())
    assertEquals("Counts should increment after fetch", initialAllTopicsCount+2, BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.count())
  }
}
