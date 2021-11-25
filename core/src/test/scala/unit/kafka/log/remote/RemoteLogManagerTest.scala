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

import kafka.cluster.Partition
import kafka.log._
import kafka.server._
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage._
import org.easymock.EasyMock
import org.easymock.EasyMock._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.{Collections, Optional, Properties}
import java.{lang, util}
import scala.collection.Seq

class RemoteLogManagerTest {

  val clusterId = "test-cluster-id"
  val brokerId = 0
  val topicPartition = new TopicPartition("test-topic", 0)
  val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val logsDir: String = Files.createTempDirectory("kafka-").toString
  val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    private var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs
  }
  val cache = new LeaderEpochFileCache(topicPartition, checkpoint)
  val rlmConfig: RemoteLogManagerConfig = createRLMConfig()

  @AfterEach
  def afterEach(): Unit = {
    Utils.delete(Paths.get(logsDir).toFile)
  }

  @Test
  def testRLMConfig(): Unit = {
    val key = "hello"
    val rlmmConfigPrefix = RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
    val props: Properties = new Properties()
    props.put(rlmmConfigPrefix + key, "world")
    props.put("remote.log.metadata.y", "z")

    val rlmConfig = createRLMConfig(props)
    val rlmmConfig = rlmConfig.remoteLogMetadataManagerProps()
    assertEquals(props.get(rlmmConfigPrefix + key), rlmmConfig.get(key))
    assertFalse(rlmmConfig.containsKey("remote.log.metadata.y"))
  }

  @Test
  def testFindHighestRemoteOffsetOnEmptyRemoteStorage(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 500)

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache))

    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()

    replay(log, rlmmManager)
    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    assertEquals(-1L, remoteLogManager.findHighestRemoteOffset(topicIdPartition))
  }

  @Test
  def testFindHighestRemoteOffset(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 500)

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache))

    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt())).andAnswer(() => {
      val epoch = getCurrentArgument[Int](1)
      if (epoch == 0) Optional.of(200) else Optional.empty()
    }).anyTimes()

    replay(log, rlmmManager)
    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    assertEquals(200L, remoteLogManager.findHighestRemoteOffset(topicIdPartition))
  }

  @Test
  def testFindNextSegmentMetadata(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 30)
    cache.assign(2, 100)

    val log: Log = createNiceMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.remoteLogEnabled()).andReturn(true).anyTimes()

    val nextSegmentLeaderEpochs = new util.HashMap[Integer, lang.Long]
    nextSegmentLeaderEpochs.put(0, 0)
    nextSegmentLeaderEpochs.put(1, 30)
    nextSegmentLeaderEpochs.put(2, 100)
    val nextSegmentMetadata: RemoteLogSegmentMetadata =
      new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        100, 199, -1L, brokerId, -1L, 1024, nextSegmentLeaderEpochs)
    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.remoteLogSegmentMetadata(EasyMock.eq(topicIdPartition), anyInt(), anyLong()))
      .andAnswer(() => {
        val epoch = getCurrentArgument[Int](1)
        val nextOffset = getCurrentArgument[Long](2)
        if (epoch == 2 && nextOffset >= 100L && nextOffset <= 199L)
          Optional.of(nextSegmentMetadata)
        else
          Optional.empty()
      }).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(Collections.emptyIterator()).anyTimes()

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = createMock(classOf[Partition])
    expect(partition.topic).andReturn(topic).anyTimes()
    expect(partition.topicPartition).andReturn(topicIdPartition.topicPartition()).anyTimes()
    expect(partition.log).andReturn(Option(log)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(0).anyTimes()
    replay(log, partition, rlmmManager)

    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    val segmentLeaderEpochs = new util.HashMap[Integer, lang.Long]()
    segmentLeaderEpochs.put(0, 0)
    segmentLeaderEpochs.put(1, 30)
    // end offset is set to 99, the next offset to search in remote storage is 100
    var segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 99, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(Option(nextSegmentMetadata), remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    // end offset is set to 105, the next offset to search in remote storage is 106
    segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 105, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(Option(nextSegmentMetadata), remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 200, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(None, remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    verify(log, partition, rlmmManager)
  }

  @Test
  def testAddAbortedTransactions(): Unit = {
    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.OFFSET))).andReturn(new FileInputStream(offsetIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TIMESTAMP))).andReturn(new FileInputStream(timeIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TRANSACTION))).andReturn(new FileInputStream(txnIdx.file)).anyTimes()

    val records: Records = createNiceMock(classOf[Records])
    expect(records.sizeInBytes()).andReturn(150).anyTimes()
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, Log.UnknownOffset, 0), records)

    var upperBoundOffsetCapture: Option[Long] = None

    replay(rsmManager, records)
    val remoteLogManager =
      new RemoteLogManager(_ => None, (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def collectAbortedTransactions(startOffset: Long,
                                                                upperBoundOffset: Long,
                                                                segmentMetadata: RemoteLogSegmentMetadata,
                                                                accumulator: List[AbortedTxn] => Unit): Unit = {
          upperBoundOffsetCapture = Option(upperBoundOffset)
        }
      }

    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))

    // If base-offset=45 and fetch-size=150, then the upperBoundOffset=47
    val actualFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)
    assertTrue(actualFetchDataInfo.abortedTransactions.isDefined)
    assertTrue(actualFetchDataInfo.abortedTransactions.get.isEmpty)
    assertEquals(Option(47), upperBoundOffsetCapture)

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    upperBoundOffsetCapture = None
    reset(records)
    expect(records.sizeInBytes()).andReturn(301).anyTimes()
    replay(records)
    remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)
    assertEquals(Option(100), upperBoundOffsetCapture)
  }

  @Test
  def testCollectAbortedTransactionsIteratesNextRemoteSegment(): Unit = {
    cache.assign(0, 0)

    val log: Log = createNiceMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.logSegments).andReturn(Iterable.empty).anyTimes()
    expect(log.remoteLogEnabled()).andReturn(true).anyTimes()

    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val nextTxnIdx = new TransactionIndex(100L, TestUtils.tempFile())
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 50, lastOffset = 105, lastStableOffset = 60),
      new AbortedTxn(producerId = 1L, firstOffset = 55, lastOffset = 120, lastStableOffset = 100)
    )
    abortedTxns.foreach(nextTxnIdx.append)

    val nextSegmentMetadata: RemoteLogSegmentMetadata =
      new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        100, 199, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 100))
    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.remoteLogSegmentMetadata(EasyMock.eq(topicIdPartition), anyInt(), anyLong()))
      .andAnswer(() => {
        val epoch = getCurrentArgument[Int](1)
        val nextOffset = getCurrentArgument[Long](2)
        if (epoch == 0 && nextOffset >= 100L && nextOffset <= 199L)
          Optional.of(nextSegmentMetadata)
        else
          Optional.empty()
      }).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(Collections.emptyIterator()).anyTimes()

    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.OFFSET))).andReturn(new FileInputStream(offsetIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TIMESTAMP))).andReturn(new FileInputStream(timeIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TRANSACTION))).andAnswer(() => {
      val segmentMetadata = getCurrentArgument[RemoteLogSegmentMetadata](0)
      if (segmentMetadata.equals(nextSegmentMetadata)) {
        new FileInputStream(nextTxnIdx.file)
      } else {
        new FileInputStream(txnIdx.file)
      }
    }).anyTimes()

    val records: Records = createNiceMock(classOf[Records])
    expect(records.sizeInBytes()).andReturn(301).anyTimes()
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, Log.UnknownOffset, 0), records)

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = createMock(classOf[Partition])
    expect(partition.topic).andReturn(topic).anyTimes()
    expect(partition.topicPartition).andReturn(topicIdPartition.topicPartition()).anyTimes()
    expect(partition.log).andReturn(Option(log)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(0).anyTimes()

    replay(log, rlmmManager, rsmManager, records, partition)
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))
    val expectedFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)

    assertTrue(expectedFetchDataInfo.abortedTransactions.isDefined)
    assertEquals(abortedTxns.map(_.asAbortedTransaction), expectedFetchDataInfo.abortedTransactions.get)

    verify(log, rlmmManager, rsmManager, records, partition)
  }

  @Test
  def testCollectAbortedTransactionsIteratesNextLocalSegment(): Unit = {
    cache.assign(0, 0)

    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val nextTxnIdx = new TransactionIndex(100L, TestUtils.tempFile())
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 50, lastOffset = 105, lastStableOffset = 60),
      new AbortedTxn(producerId = 1L, firstOffset = 55, lastOffset = 120, lastStableOffset = 100)
    )
    abortedTxns.foreach(nextTxnIdx.append)

    val logSegment: LogSegment = createNiceMock(classOf[LogSegment])
    expect(logSegment.txnIndex).andReturn(nextTxnIdx).anyTimes()

    val log: Log = createNiceMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.logSegments).andReturn(List(logSegment)).anyTimes()
    expect(log.remoteLogEnabled()).andReturn(true).anyTimes()

    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.remoteLogSegmentMetadata(EasyMock.eq(topicIdPartition), anyInt(), anyLong()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(Collections.emptyIterator()).anyTimes()

    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.OFFSET))).andReturn(new FileInputStream(offsetIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TIMESTAMP))).andReturn(new FileInputStream(timeIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TRANSACTION))).andReturn(new FileInputStream(txnIdx.file)).anyTimes()

    val records: Records = createNiceMock(classOf[Records])
    expect(records.sizeInBytes()).andReturn(301).anyTimes()
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, Log.UnknownOffset, 0), records)

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = createMock(classOf[Partition])
    expect(partition.topic).andReturn(topic).anyTimes()
    expect(partition.topicPartition).andReturn(topicIdPartition.topicPartition()).anyTimes()
    expect(partition.log).andReturn(Option(log)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(0).anyTimes()

    replay(logSegment, log, rlmmManager, rsmManager, records, partition)
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))
    val expectedFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)

    assertTrue(expectedFetchDataInfo.abortedTransactions.isDefined)
    assertEquals(abortedTxns.map(_.asAbortedTransaction), expectedFetchDataInfo.abortedTransactions.get)

    verify(logSegment, log, rlmmManager, rsmManager, records, partition)
  }

  @Test
  def testGetClassLoaderAwareRemoteStorageManager(): Unit = {
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    val remoteLogManager =
      new RemoteLogManager(_ => None, (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    assertEquals(rsmManager, remoteLogManager.storageManager())
  }

  private def nonExistentTempFile(): File = {
    val file = TestUtils.tempFile()
    file.delete()
    file
  }

  private def createRLMConfig(props: Properties = new Properties): RemoteLogManagerConfig = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props)
    new RemoteLogManagerConfig(config)
  }
}