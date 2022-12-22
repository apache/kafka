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
import kafka.log.UnifiedLog
import kafka.server.KafkaConfig
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.MockTime
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.internals.{OffsetIndex, TimeIndex}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage._
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}

import java.io.{ByteArrayInputStream, File, FileInputStream}
import java.nio.file.Files
import java.util
import java.util.{Optional, Properties}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class RemoteLogManagerTest {

  val time = new MockTime()
  val brokerId = 0
  val logDir: String = TestUtils.tempDirectory("kafka-").toString

  val remoteStorageManager: RemoteStorageManager = mock(classOf[RemoteStorageManager])
  val remoteLogMetadataManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
  var remoteLogManagerConfig: RemoteLogManagerConfig = _
  var remoteLogManager: RemoteLogManager = _

  val leaderTopicIdPartition =  new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Leader", 0))
  val followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Follower", 0))
  val topicIds: util.Map[String, Uuid] = Map(
    leaderTopicIdPartition.topicPartition().topic() -> leaderTopicIdPartition.topicId(),
    followerTopicIdPartition.topicPartition().topic() -> followerTopicIdPartition.topicId()
  ).asJava

  val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs
  }

  @BeforeEach
  def setUp(): Unit = {
    val props = new Properties()
    remoteLogManagerConfig = createRLMConfig(props)
    remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir) {
      override private[remote] def createRemoteStorageManager() = remoteStorageManager
      override private[remote] def createRemoteLogMetadataManager() = remoteLogMetadataManager
    }
  }

  @Test
  def testRemoteLogMetadataManagerWithUserDefinedConfigs(): Unit = {
    val key = "key"
    val configPrefix = "config.prefix"
    val props: Properties = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, configPrefix)
    props.put(configPrefix + key, "world")
    props.put("remote.log.metadata.y", "z")

    val metadataMangerConfig = createRLMConfig(props).remoteLogMetadataManagerProps()
    assertEquals(props.get(configPrefix + key), metadataMangerConfig.get(key))
    assertFalse(metadataMangerConfig.containsKey("remote.log.metadata.y"))
  }

  @Test
  def testStartup(): Unit = {
    remoteLogManager.startup()
    val capture: ArgumentCaptor[util.Map[String, _]] = ArgumentCaptor.forClass(classOf[util.Map[String, _]])
    verify(remoteStorageManager, times(1)).configure(capture.capture())
    assertEquals(brokerId, capture.getValue.get(KafkaConfig.BrokerIdProp))

    verify(remoteLogMetadataManager, times(1)).configure(capture.capture())
    assertEquals(brokerId, capture.getValue.get(KafkaConfig.BrokerIdProp))
    assertEquals(logDir, capture.getValue.get(KafkaConfig.LogDirProp))
  }

  @Test
  def testGetClassLoaderAwareRemoteStorageManager(): Unit = {
    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    val remoteLogManager =
      new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    assertEquals(rsmManager, remoteLogManager.storageManager())
  }

  @Test
  def testTopicIdCacheUpdates(): Unit = {
    def verifyInCache(topicIdPartitions: TopicIdPartition*): Unit = {
      topicIdPartitions.foreach { topicIdPartition =>
        assertDoesNotThrow(() =>
          remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), epochForOffset = 0, offset = 0L))
      }
    }

    def verifyNotInCache(topicIdPartitions: TopicIdPartition*): Unit = {
      topicIdPartitions.foreach { topicIdPartition =>
        assertThrows(classOf[KafkaException], () =>
          remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), epochForOffset = 0, offset = 0L))
      }
    }

    val mockLeaderPartition = mockPartition(leaderTopicIdPartition)
    val mockFollowerPartition = mockPartition(followerTopicIdPartition)

    when(remoteLogMetadataManager.remoteLogSegmentMetadata(any(classOf[TopicIdPartition]), anyInt(), anyLong()))
      .thenReturn(Optional.empty[RemoteLogSegmentMetadata]())
    verifyNotInCache(followerTopicIdPartition, leaderTopicIdPartition)
    // Load topicId cache
    remoteLogManager.onLeadershipChange(Set(mockLeaderPartition), Set(mockFollowerPartition), topicIds)
    verify(remoteLogMetadataManager, times(1))
      .onPartitionLeadershipChanges(Set(leaderTopicIdPartition).asJava, Set(followerTopicIdPartition).asJava)
    verifyInCache(followerTopicIdPartition, leaderTopicIdPartition)

    // Evicts from topicId cache
    remoteLogManager.stopPartitions(leaderTopicIdPartition.topicPartition(), delete = true)
    verifyNotInCache(leaderTopicIdPartition)
    verifyInCache(followerTopicIdPartition)

    // Evicts from topicId cache
    remoteLogManager.stopPartitions(followerTopicIdPartition.topicPartition(), delete = true)
    verifyNotInCache(leaderTopicIdPartition, followerTopicIdPartition)
  }

  @Test
  def testFetchRemoteLogSegmentMetadata(): Unit = {
    remoteLogManager.onLeadershipChange(
      Set(mockPartition(leaderTopicIdPartition)), Set(mockPartition(followerTopicIdPartition)), topicIds)
    remoteLogManager.fetchRemoteLogSegmentMetadata(leaderTopicIdPartition.topicPartition(), 10, 100L)
    remoteLogManager.fetchRemoteLogSegmentMetadata(followerTopicIdPartition.topicPartition(), 20, 200L)

    verify(remoteLogMetadataManager)
      .remoteLogSegmentMetadata(ArgumentMatchers.eq(leaderTopicIdPartition), anyInt(), anyLong())
    verify(remoteLogMetadataManager)
      .remoteLogSegmentMetadata(ArgumentMatchers.eq(followerTopicIdPartition), anyInt(), anyLong())
  }

  @Test
  def testFindOffsetByTimestamp(): Unit = {
    val tp = leaderTopicIdPartition.topicPartition()
    val remoteLogSegmentId = new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid())
    val ts = time.milliseconds()
    val startOffset = 120
    val targetLeaderEpoch = 10

    val segmentMetadata = mock(classOf[RemoteLogSegmentMetadata])
    when(segmentMetadata.remoteLogSegmentId()).thenReturn(remoteLogSegmentId)
    when(segmentMetadata.maxTimestampMs()).thenReturn(ts + 2)
    when(segmentMetadata.startOffset()).thenReturn(startOffset)
    when(segmentMetadata.endOffset()).thenReturn(startOffset + 2)

    val tpDir: File = new File(logDir, tp.toString)
    Files.createDirectory(tpDir.toPath)
    val txnIdxFile = new File(tpDir, "txn-index" + UnifiedLog.TxnIndexFileSuffix)
    txnIdxFile.createNewFile()
    when(remoteStorageManager.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer { ans =>
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val maxEntries = (metadata.endOffset() - metadata.startOffset()).asInstanceOf[Int]
        val offsetIdx = new OffsetIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.IndexFileSuffix),
          metadata.startOffset(), maxEntries * 8)
        val timeIdx = new TimeIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.TimeIndexFileSuffix),
          metadata.startOffset(), maxEntries * 12)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          case IndexType.TRANSACTION => new FileInputStream(txnIdxFile)
          case IndexType.LEADER_EPOCH =>
          case IndexType.PRODUCER_SNAPSHOT =>
        }
      }

    when(remoteLogMetadataManager.listRemoteLogSegments(ArgumentMatchers.eq(leaderTopicIdPartition), anyInt()))
      .thenAnswer(ans => {
        val leaderEpoch = ans.getArgument[Int](1)
        if (leaderEpoch == targetLeaderEpoch)
          List(segmentMetadata).asJava.iterator()
        else
          List().asJava.iterator()
      })

    def records(timestamp: Long,
                initialOffset: Long,
                partitionLeaderEpoch: Int): MemoryRecords = {
      MemoryRecords.withRecords(initialOffset, CompressionType.NONE, partitionLeaderEpoch,
        new SimpleRecord(timestamp - 1, "first message".getBytes()),
        new SimpleRecord(timestamp + 1, "second message".getBytes()),
        new SimpleRecord(timestamp + 2, "third message".getBytes()),
      )
    }

    // 3 messages are added with offset, and timestamp as below
    // startOffset   , ts-1
    // startOffset+1 , ts+1
    // startOffset+2 , ts+2
    when(remoteStorageManager.fetchLogSegment(segmentMetadata, 0))
      .thenAnswer(_ => new ByteArrayInputStream(records(ts, startOffset, targetLeaderEpoch).buffer().array()))

    val leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint)
    leaderEpochFileCache.assign(epoch = 5, startOffset = 99L)
    leaderEpochFileCache.assign(epoch = targetLeaderEpoch, startOffset = startOffset)
    leaderEpochFileCache.assign(epoch = 12, startOffset = 500L)

    remoteLogManager.onLeadershipChange(Set(mockPartition(leaderTopicIdPartition)), Set(), topicIds)
    // Fetching message for timestamp `ts` will return the message with startOffset+1, and `ts+1` as there are no
    // messages starting with the startOffset and with `ts`.
    val maybeTimestampAndOffset1 = remoteLogManager.findOffsetByTimestamp(tp, ts, startOffset, leaderEpochFileCache)
    assertEquals(Some(new TimestampAndOffset(ts + 1, startOffset + 1, Optional.of(targetLeaderEpoch))), maybeTimestampAndOffset1)

    // Fetching message for `ts+2` will return the message with startOffset+2 and its timestamp value is `ts+2`.
    val maybeTimestampAndOffset2 = remoteLogManager.findOffsetByTimestamp(tp, ts + 2, startOffset, leaderEpochFileCache)
    assertEquals(Some(new TimestampAndOffset(ts + 2, startOffset + 2, Optional.of(targetLeaderEpoch))), maybeTimestampAndOffset2)

    // Fetching message for `ts+3` will return None as there are no records with timestamp >= ts+3.
    val maybeTimestampAndOffset3 = remoteLogManager.findOffsetByTimestamp(tp, ts + 3, startOffset, leaderEpochFileCache)
    assertEquals(None, maybeTimestampAndOffset3)
  }

  @Test
  def testIdempotentClose(): Unit = {
    remoteLogManager.close()
    remoteLogManager.close()
    val inorder = inOrder(remoteStorageManager, remoteLogMetadataManager)
    inorder.verify(remoteStorageManager, times(1)).close()
    inorder.verify(remoteLogMetadataManager, times(1)).close()
  }

  private def mockPartition(topicIdPartition: TopicIdPartition) = {
    val tp = topicIdPartition.topicPartition()
    val partition: Partition = mock(classOf[Partition])
    val log = mock(classOf[UnifiedLog])
    when(partition.topicPartition).thenReturn(tp)
    when(partition.topic).thenReturn(tp.topic())
    when(log.remoteLogEnabled()).thenReturn(true)
    when(partition.log).thenReturn(Some(log))
    partition
  }

  private def createRLMConfig(props: Properties): RemoteLogManagerConfig = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props)
    new RemoteLogManagerConfig(config)
  }

}
