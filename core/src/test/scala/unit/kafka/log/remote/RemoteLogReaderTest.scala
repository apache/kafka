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

import java.nio.file.Files
import java.util.{Optional, Properties}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{CompletableFuture, RejectedExecutionException}

import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchTxnCommitted, LogOffsetMetadata, RemoteStorageFetchInfo}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class RemoteLogReaderTest {

  @Test
  def testReadRemoteLog(): Unit = {
    val rlm = new MockRemoteLogManager(2, 10)
    val tp = new TopicPartition("test", 0)
    val fetchInfo = new PartitionData(200, 200, 1000, Optional.of(1))
    val resultFuture = new CompletableFuture[RemoteLogReadResult]
    def callback(result: RemoteLogReadResult): Unit = {
      resultFuture.complete(result)
    }
    rlm.asyncRead(RemoteStorageFetchInfo(1000, minOneMessage = true, tp, fetchInfo, FetchTxnCommitted), callback)
    Thread.sleep(100)

    val dataInfo = resultFuture.get.info.get
    assertEquals(None, resultFuture.get.error)
    assertEquals(200, dataInfo.fetchOffsetMetadata.messageOffset)
    assertEquals(2, dataInfo.records.records.asScala.size)
    assertEquals(202, dataInfo.records.records.iterator.next.offset)
  }

  @Test
  def testTaskQueueFullAndCancelTask(): Unit = {
    val rlm = new MockRemoteLogManager(5, 20)
    rlm.pause()

    val tp = new TopicPartition("test", 0)
    val tasks = new Array[RemoteLogManager#AsyncReadTask](26)
    val finishedTasks = Array.fill[Boolean](26)(false)
    val finishCount = new AtomicInteger(0)
    def callback(result: RemoteLogReadResult): Unit = {
      assertEquals(None, result.error)
      finishCount.addAndGet(1)
      finishedTasks(result.info.get.fetchOffsetMetadata.messageOffset.asInstanceOf[Int]) = true
    }

    for (i <- 0 to 24) {
      val fetchInfo = new PartitionData(i, 0, 1000, Optional.of(1))
      val future = rlm.asyncRead(RemoteStorageFetchInfo(1000, minOneMessage = true, tp, fetchInfo, FetchTxnCommitted), callback)
      tasks(i) = future
    }
    assertEquals(0, finishCount.get)

    assertThrows(classOf[RejectedExecutionException], () => {
      val fetchInfo = new PartitionData(25, 0, 1000, Optional.of(1))
      rlm.asyncRead(RemoteStorageFetchInfo(1000, minOneMessage = true, tp, fetchInfo, FetchTxnCommitted), callback)
    })

    tasks(7).cancel(false)
    tasks(10).cancel(true)

    val fetchInfo = new PartitionData(25, 0, 1000, Optional.of(1))
    rlm.asyncRead(RemoteStorageFetchInfo(1000, minOneMessage = true, tp, fetchInfo, FetchTxnCommitted), callback)
    rlm.resume()

    Thread.sleep(200)
    assertEquals(24, finishCount.get)
    assertEquals(24, finishedTasks.count(a => a))
    assertEquals(false, finishedTasks(7))
    assertEquals(false, finishedTasks(10))
  }

  @Test
  def testErr(): Unit = {
    val rlm = new MockRemoteLogManager(2, 10) {
      override def read(remoteStorageFetchInfo: RemoteStorageFetchInfo): FetchDataInfo = {
        throw new OffsetOutOfRangeException("Offset: %d is out of range"
          .format(remoteStorageFetchInfo.fetchInfo.fetchOffset))
      }
    }
    val tp = new TopicPartition("test", 1)
    val fetchInfo = new PartitionData(10000, 0, 1000, Optional.of(1))
    val resultFuture = new CompletableFuture[RemoteLogReadResult]
    def callback(result: RemoteLogReadResult): Unit = {
      resultFuture.complete(result)
    }

    val task = rlm.asyncRead(RemoteStorageFetchInfo(1000, minOneMessage = true, tp, fetchInfo, FetchTxnCommitted), callback)
    Thread.sleep(100)
    assertTrue(task.isDone)
    assertEquals(None, resultFuture.get.info)
    assertEquals(classOf[OffsetOutOfRangeException], resultFuture.get.error.get.getClass)
  }
}

class MockRemoteLogManager(threads: Int, taskQueueSize: Int)
  extends RemoteLogManager(
    _ => None,
    (_, _) => {},
    MockRemoteLogManager.rlmConfig(threads, taskQueueSize),
    new SystemTime,
    1,
    "mock-cluster-id",
    Files.createTempDirectory("kafka-test-").toString,
    new BrokerTopicStats) {

  private val lock = new ReentrantReadWriteLock

  override def read(remoteStorageFetchInfo: RemoteStorageFetchInfo): FetchDataInfo = {
    lock.readLock.lock()
    try {
      val fetchInfo = remoteStorageFetchInfo.fetchInfo
      val recordsArray = Array(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      )
      val records = MemoryRecords.withRecords(fetchInfo.fetchOffset + 2, CompressionType.NONE, 1, recordsArray: _*)
      FetchDataInfo(new LogOffsetMetadata(fetchInfo.fetchOffset), records)
    } finally {
      lock.readLock.unlock()
    }
  }

  def pause(): Unit = {
    lock.writeLock.lock()
  }

  def resume(): Unit = {
    lock.writeLock.unlock()
  }
}

object MockRemoteLogManager {

  def rlmConfig(threads: Int, taskQueueSize: Int): RemoteLogManagerConfig = {
    val props = new Properties
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, "rlmm.config.")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, "rsm.config.")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "kafka.log.remote.MockRemoteStorageManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "kafka.log.remote.MockRemoteLogMetadataManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, threads.toString)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP, taskQueueSize.toString)
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props, false)
    new RemoteLogManagerConfig(config)
  }
}