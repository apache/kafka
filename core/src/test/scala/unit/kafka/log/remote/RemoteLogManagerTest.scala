/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import java.util.{Collections, Optional, Properties}

import kafka.log.remote.RemoteLogManager.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX
import kafka.log.{CleanerConfig, Log, LogConfig, LogManager, LogSegment}
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server._
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}

class RemoteLogManagerTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val metrics = new Metrics

  val rsmConfig: Map[String, Any] = Map(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "url" -> "foo.url",
    REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "timout.ms" -> 1000L)
  val rlmConfig = RemoteLogManagerConfig(remoteLogStorageEnable = true, "kafka.log.remote.MockRemoteStorageManager", "",
    1024, 60000, 2, 10, rsmConfig, 10, 30000)

  var logConfig: LogConfig = _
  var tmpDir: File = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = _
  var rlmMock: RemoteLogManager = EasyMock.createMock(classOf[RemoteLogManager])

  @Before
  def setup(): Unit = {
    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    val logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    val logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig,
      CleanerConfig(enableCleaner = false), time, rlmConfig)
    logManager.startup()

    val brokerProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    brokerProps.put(KafkaConfig.LogDirsProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    val brokerConfig = KafkaConfig.fromProps(brokerProps)
    val kafkaZkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    val quotaManagers = QuotaFactory.instantiate(brokerConfig, metrics, time, "")
    replicaManager = new ReplicaManager(
      config = brokerConfig, metrics, time, zkClient = kafkaZkClient, new MockScheduler(time),
      logManager, Option(rlmMock), new AtomicBoolean(false), quotaManagers,
      brokerTopicStats, new MetadataCache(brokerId), new LogDirFailureChannel(brokerConfig.logDirs.size))

    EasyMock.expect(kafkaZkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(
      logProps).anyTimes()
    EasyMock.expect(
      kafkaZkClient.conditionalUpdatePath(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
        EasyMock.anyObject()))
      .andReturn((true, 0)).anyTimes()
    EasyMock.replay(kafkaZkClient)
  }

  @After
  def tearDown(): Unit = {
    EasyMock.reset(rlmMock)
    brokerTopicStats.close()
    metrics.close()

    logManager.shutdown()
    Utils.delete(tmpDir)
    logManager.liveLogDirs.foreach(Utils.delete)
    replicaManager.shutdown(checkpointHW = false)
  }

  private def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 300000: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @Test
  def testRSMConfigInvocation() {

    def logFetcher(tp: TopicPartition): Option[Log] = logManager.getLog(tp)

    def lsoUpdater(tp: TopicPartition, los: Long): Unit = {}

    // this should initialize RSM
    new RemoteLogManager(logFetcher, lsoUpdater, rlmConfig, time)

    assertTrue(rsmConfig.count { case (k, v) => MockRemoteStorageManager.configs.get(k) == v } == rsmConfig.size)
    assertEquals(MockRemoteStorageManager.configs.get(KafkaConfig.RemoteLogRetentionBytesProp),
      rlmConfig.remoteLogRetentionBytes)
    assertEquals(MockRemoteStorageManager.configs.get(KafkaConfig.RemoteLogRetentionMillisProp),
      rlmConfig.remoteLogRetentionMillis)
  }

  @Test
  def testRemoteLogRecordsFetch() {
    val lastOffset = 5
    // return the lastOffset to verify when out of range offsets are requested.
    EasyMock.expect(rlmMock.close()).anyTimes()
    EasyMock.replay(rlmMock)

    val leaderEpoch = 1
    val partition = replicaManager.createPartition(topicPartition)

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(1)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Collections.singletonList(brokerId))
      .setZkVersion(1)
      .setReplicas(Collections.singletonList(brokerId))
      .setIsNew(true)
    partition.makeLeader(1, leaderState, 1, new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints))

    val recordsArray = Array(new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes))
    val inputRecords: util.List[SimpleRecord] = util.Arrays.asList(recordsArray: _*)

    val inputMemoryRecords = Map(topicPartition -> MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
      recordsArray: _*))

    replicaManager.appendRecords(timeout = 30000, requiredAcks = 1, internalTopicsAllowed = true, isFromClient = true,
      entriesPerPartition = inputMemoryRecords, responseCallback = _ => ())

    def logReadResultFor(fetchOffset: Long): LogReadResult = {
      val partitionInfo = Seq((topicPartition, new PartitionData(fetchOffset, 0, 1000,
        Optional.of(leaderEpoch))))
      val readRecords = replicaManager.readFromLocalLog(100, fetchOnlyFromLeader = true, FetchTxnCommitted, 1000,
        hardMaxBytesLimit = false, partitionInfo, UnboundedQuota, None)
      if (readRecords.isEmpty) null else readRecords.last._2
    }

    val logReadResult_0 = logReadResultFor(0L)
    val receivedRecords = logReadResult_0.info.records
    // check the records are same
    val result = new util.ArrayList[SimpleRecord]()
    receivedRecords.records().forEach(new Consumer[Record] {
      override def accept(t: Record): Unit = result.add(new SimpleRecord(t))
    })
    assertEquals(inputRecords, result)

    val outOfRangeOffset = logManager.getLog(topicPartition).get.logEndOffset + 1

    // fetching offsets beyond local log would result in fetching from remote log, it is mocked to return lastOffset,
    //nextLocalOffset should be lastOffset +1
    val logReadResult = logReadResultFor(outOfRangeOffset)
    // fetch response should have no records as it is to indicate that the requested fetch messages are in
    // remote tier and the next offset available locally is sent as `nextLocalOffset` so that follower replica can
    // start fetching that for local storage.
    assertTrue(logReadResult.info.records.sizeInBytes() == 0)
  }

}

object MockRemoteStorageManager {
  var configs: util.Map[String, _] = _
}

class MockRemoteStorageManager extends RemoteStorageManager {

  override def copyLogSegment(topicPartition: TopicPartition, logSegment: LogSegment,
                              leaderEpoch: Int): util.List[RemoteLogIndexEntry] = Collections.emptyList()

  override def listRemoteSegments(topicPartition: TopicPartition,
                                  minBaseOffset: Long): util.List[RemoteLogSegmentInfo] = Collections.emptyList()

  override def getRemoteLogIndexEntries(remoteLogSegment: RemoteLogSegmentInfo): util.List[RemoteLogIndexEntry] = Collections.emptyList()

  override def deleteLogSegment(remoteLogSegment: RemoteLogSegmentInfo): Boolean = true

  override def deleteTopicPartition(topicPartition: TopicPartition): Boolean = true

  override def read(remoteLogIndexEntry: RemoteLogIndexEntry, maxBytes: Int, startOffset: Long,
                    minOneMessage: Boolean): Records = MemoryRecords.EMPTY

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    MockRemoteStorageManager.configs = configs
  }

  override def cleanupLogUntil(topicPartition: TopicPartition, cleanUpTillMs: Long): Long = 0L

  override def earliestLogOffset(tp: TopicPartition): Long = 0L
}