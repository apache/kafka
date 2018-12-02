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

import java.io.File

import kafka.api._
import kafka.utils._
import kafka.cluster.Replica
import kafka.log.{Log, LogManager}
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.junit.{After, Before, Test}
import java.util.{Optional, Properties}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
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
  val metrics = new Metrics
  val leaderLEO = 20L
  val followerLEO = 15L
  val partitionHW = 5

  val fetchSize = 100
  val recordToHW = new SimpleRecord("recordToHW".getBytes())
  val recordToLEO = new SimpleRecord("recordToLEO".getBytes())

  val topic = "test-topic"
  val partitionId = 0
  val topicPartition = new TopicPartition(topic, partitionId)

  val fetchInfo = Seq(topicPartition -> new PartitionData(0, 0, fetchSize,
    Optional.empty()))

  var replicaManager: ReplicaManager = _

  @Before
  def setUp() {
    // create nice mock since we don't particularly care about zkclient calls
    val kafkaZkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
    EasyMock.replay(kafkaZkClient)

    // create nice mock since we don't particularly care about scheduler calls
    val scheduler: KafkaScheduler = EasyMock.createNiceMock(classOf[KafkaScheduler])
    EasyMock.replay(scheduler)

    // create the log which takes read with either HW max offset or none max offset
    val log: Log = EasyMock.createNiceMock(classOf[Log])
    EasyMock.expect(log.logStartOffset).andReturn(0).anyTimes()
    EasyMock.expect(log.logEndOffset).andReturn(leaderLEO).anyTimes()
    EasyMock.expect(log.dir).andReturn(TestUtils.tempDir()).anyTimes()
    EasyMock.expect(log.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(leaderLEO)).anyTimes()
    EasyMock.expect(log.read(
      startOffset = 0,
      maxLength = fetchSize,
      maxOffset = Some(partitionHW),
      minOneMessage = true,
      includeAbortedTxns = false))
      .andReturn(FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(CompressionType.NONE, recordToHW)
      )).anyTimes()
    EasyMock.expect(log.read(
      startOffset = 0,
      maxLength = fetchSize,
      maxOffset = None,
      minOneMessage = true,
      includeAbortedTxns = false))
      .andReturn(FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(CompressionType.NONE, recordToLEO)
      )).anyTimes()
    EasyMock.replay(log)

    // create the log manager that is aware of this mock log
    val logManager: LogManager = EasyMock.createMock(classOf[LogManager])
    EasyMock.expect(logManager.getLog(topicPartition, false)).andReturn(Some(log)).anyTimes()
    EasyMock.expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    EasyMock.replay(logManager)

    // create the replica manager
    replicaManager = new ReplicaManager(configs.head, metrics, time, kafkaZkClient, scheduler, logManager,
      new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(configs.head.brokerId), new LogDirFailureChannel(configs.head.logDirs.size))

    // add the partition with two replicas, both in ISR
    val partition = replicaManager.getOrCreatePartition(new TopicPartition(topic, partitionId))

    // create the leader replica with the local log
    val leaderReplica = new Replica(configs.head.brokerId, partition.topicPartition, time, 0, Some(log))
    leaderReplica.highWatermark = new LogOffsetMetadata(partitionHW)
    partition.leaderReplicaIdOpt = Some(leaderReplica.brokerId)

    // create the follower replica with defined log end offset
    val followerReplica= new Replica(configs(1).brokerId, partition.topicPartition, time)
    val leo = new LogOffsetMetadata(followerLEO, 0L, followerLEO.toInt)
    followerReplica.updateLogReadResult(new LogReadResult(info = FetchDataInfo(leo, MemoryRecords.EMPTY),
                                                          highWatermark = leo.messageOffset,
                                                          leaderLogStartOffset = 0L,
                                                          leaderLogEndOffset = leo.messageOffset,
                                                          followerLogStartOffset = 0L,
                                                          fetchTimeMs = time.milliseconds,
                                                          readSize = -1,
                                                          lastStableOffset = None))

    // add both of them to ISR
    val allReplicas = List(leaderReplica, followerReplica)
    allReplicas.foreach(partition.addReplicaIfNotExists)
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
    val brokerTopicStats = new BrokerTopicStats
    val initialTopicCount = brokerTopicStats.topicStats(topic).totalFetchRequestRate.count()
    val initialAllTopicsCount = brokerTopicStats.allTopicsStats.totalFetchRequestRate.count()

    val readCommittedRecords = replicaManager.readFromLocalLog(
      replicaId = Request.OrdinaryConsumerId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = UnboundedQuota).find(_._1 == topicPartition)
    val firstReadRecord = readCommittedRecords.get._2.info.records.records.iterator.next()
    assertEquals("Reading committed data should return messages only up to high watermark", recordToHW,
      new SimpleRecord(firstReadRecord))

    val readAllRecords = replicaManager.readFromLocalLog(
      replicaId = Request.OrdinaryConsumerId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchLogEnd,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = UnboundedQuota).find(_._1 == topicPartition)

    val firstRecord = readAllRecords.get._2.info.records.records.iterator.next()
    assertEquals("Reading any data can return messages up to the end of the log", recordToLEO,
      new SimpleRecord(firstRecord))

    assertEquals("Counts should increment after fetch", initialTopicCount+2, brokerTopicStats.topicStats(topic).totalFetchRequestRate.count())
    assertEquals("Counts should increment after fetch", initialAllTopicsCount+2, brokerTopicStats.allTopicsStats.totalFetchRequestRate.count())
  }
}
