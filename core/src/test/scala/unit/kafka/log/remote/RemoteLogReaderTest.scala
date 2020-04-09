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
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{CompletableFuture, RejectedExecutionException}

import kafka.log.remote.RemoteLogManager.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX
import kafka.server.{Defaults, FetchDataInfo, LogOffsetMetadata, RemoteStorageFetchInfo}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.SystemTime
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._

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

    rlm.asyncRead(RemoteStorageFetchInfo(1000, true, tp, fetchInfo), callback)

    Thread.sleep(100)

    val i = resultFuture.get.info.get
    assertEquals(None, resultFuture.get.error)
    assertEquals(200, i.fetchOffsetMetadata.messageOffset)
    assertEquals(2, i.records.records.asScala.size)
    assertEquals(202, i.records.records.iterator.next.offset)
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
      val future = rlm.asyncRead(RemoteStorageFetchInfo(1000, true, tp, fetchInfo), callback)
      tasks(i) = future
    }
    assertEquals(0, finishCount.get)

    assertThrows[RejectedExecutionException] {
      val fetchInfo = new PartitionData(25, 0, 1000, Optional.of(1))
      rlm.asyncRead(RemoteStorageFetchInfo(1000, true, tp, fetchInfo), callback)
    }

    tasks(7).cancel(false)
    tasks(10).cancel(true)

    val fetchInfo = new PartitionData(25, 0, 1000, Optional.of(1))
    rlm.asyncRead(RemoteStorageFetchInfo(1000, true, tp, fetchInfo), callback)
    rlm.resume()

    Thread.sleep(200)
    assertEquals(24, finishCount.get)
    assertEquals(24, finishedTasks.count(a => a))
    assertEquals(false, finishedTasks(7))
    assertEquals(false, finishedTasks(10))
  }

  @Test
  def testErr(): Unit = {
    val rlm = new MockRemoteLogManager(2, 10)
    val tp = new TopicPartition("test", 1)
    val fetchInfo = new PartitionData(10000, 0, 1000, Optional.of(1))

    def callback(result: RemoteLogReadResult): Unit = {
      assertEquals(None, result.info)
      assert(result.error.get.isInstanceOf[OffsetOutOfRangeException])
    }

    val task = rlm.asyncRead(RemoteStorageFetchInfo(1000, true, tp, fetchInfo), callback)
    Thread.sleep(100)
    assert(task.isDone)
  }
}

class MockRemoteLogManager(threads: Int, taskQueueSize: Int)
  extends RemoteLogManager((tp) => None, (tp, segment) => {}, MockRemoteLogManager.rlmConfig(threads, taskQueueSize),
    new SystemTime, "localhost:9092", 1, "mock-cluster",
    Files.createTempDirectory("kafka-test-").toString) {
  private val lock = new ReentrantReadWriteLock

  override def read(fetchMaxBytes: Int, minOneMessage: Boolean, tp: TopicPartition, fetchInfo: FetchRequest.PartitionData): FetchDataInfo = {
    lock.readLock.lock()
    try {
      val recordsArray = Array(new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes))
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
  val rsmConfig: Map[String, Any] = Map(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "url" -> "foo.url",
    REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "timout.ms" -> 1000L)

  def rlmConfig(threads: Int, taskQueueSize: Int): RemoteLogManagerConfig = {
    RemoteLogManagerConfig(remoteLogStorageEnable = true, "kafka.log.remote.MockRemoteStorageManager", "",
      1024, 60000, threads, taskQueueSize, rsmConfig, Defaults.RemoteLogManagerThreadPoolSize,
      Defaults.RemoteLogManagerTaskIntervalMs, "kafka.log.remote.MockRemoteLogMetadataManager")
  }
}