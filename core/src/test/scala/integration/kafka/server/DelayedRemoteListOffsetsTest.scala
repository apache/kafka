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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, TopicPartitionOperationKey}
import org.apache.kafka.server.util.timer.MockTimer
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.Mockito.{mock, when}

import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.concurrent.TimeoutException
import scala.jdk.CollectionConverters._

class DelayedRemoteListOffsetsTest {

  val delayMs = 10
  val timer = new MockTimer()
  val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  type T = Either[Exception, Option[TimestampAndOffset]]
  val purgatory =
    new DelayedOperationPurgatory[DelayedRemoteListOffsets]("test-purgatory", timer, 0, 10, true, true)

  @AfterEach
  def afterEach(): Unit = {
    purgatory.shutdown()
  }

  @Test
  def testResponseOnRequestExpiration(): Unit = {
    var numResponse = 0
    val responseCallback = (response: List[ListOffsetsTopicResponse]) => {
      response.foreach { topic =>
        topic.partitions().forEach { partition =>
          assertEquals(Errors.REQUEST_TIMED_OUT.code(), partition.errorCode())
          assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partition.timestamp())
          assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partition.offset())
          assertEquals(-1, partition.leaderEpoch())
          numResponse += 1
        }
      }
    }

    var cancelledCount = 0
    val jobFuture = mock(classOf[CompletableFuture[Void]])
    val holder: AsyncOffsetReadFutureHolder[T] = mock(classOf[AsyncOffsetReadFutureHolder[T]])
    when(holder.taskFuture).thenAnswer(_ => new CompletableFuture[T]())
    when(holder.jobFuture).thenReturn(jobFuture)
    when(jobFuture.cancel(anyBoolean())).thenAnswer(_ => {
      cancelledCount += 1
      true
    })

    val statusByPartition = mutable.Map(
      new TopicPartition("test", 0) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test", 1) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test1", 0) -> ListOffsetsPartitionStatus(None, Some(holder))
    )

    val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version = 5, statusByPartition, replicaManager, responseCallback)
    val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList.asJava
    assertEquals(0, DelayedRemoteListOffsetsMetrics.aggregateExpirationMeter.count())
    assertEquals(0, DelayedRemoteListOffsetsMetrics.partitionExpirationMeters.size)
    purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys)

    Thread.sleep(100)
    assertEquals(3, listOffsetsRequestKeys.size)
    assertEquals(listOffsetsRequestKeys.size, cancelledCount)
    assertEquals(listOffsetsRequestKeys.size, numResponse)
    assertEquals(listOffsetsRequestKeys.size, DelayedRemoteListOffsetsMetrics.aggregateExpirationMeter.count())
    listOffsetsRequestKeys.forEach(key => {
      val tp = new TopicPartition(key.topic, key.partition)
      assertEquals(1, DelayedRemoteListOffsetsMetrics.partitionExpirationMeters.get(tp).count())
    })
  }

  @Test
  def testResponseOnSuccess(): Unit = {
    var numResponse = 0
    val responseCallback = (response: List[ListOffsetsTopicResponse]) => {
      response.foreach { topic =>
        topic.partitions().forEach { partition =>
          assertEquals(Errors.NONE.code(), partition.errorCode())
          assertEquals(100L, partition.timestamp())
          assertEquals(100L, partition.offset())
          assertEquals(50, partition.leaderEpoch())
          numResponse += 1
        }
      }
    }

    val timestampAndOffset = new TimestampAndOffset(100L, 100L, Optional.of(50))
    val taskFuture = new CompletableFuture[T]()
    taskFuture.complete(Right(Some(timestampAndOffset)))

    var cancelledCount = 0
    val jobFuture = mock(classOf[CompletableFuture[Void]])
    val holder: AsyncOffsetReadFutureHolder[T] = mock(classOf[AsyncOffsetReadFutureHolder[T]])
    when(holder.taskFuture).thenAnswer(_ => taskFuture)
    when(holder.jobFuture).thenReturn(jobFuture)
    when(jobFuture.cancel(anyBoolean())).thenAnswer(_ => {
      cancelledCount += 1
      true
    })

    val statusByPartition = mutable.Map(
      new TopicPartition("test", 0) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test", 1) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test1", 0) -> ListOffsetsPartitionStatus(None, Some(holder))
    )

    val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version = 5, statusByPartition, replicaManager, responseCallback)
    val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList.asJava
    purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys)

    assertEquals(0, cancelledCount)
    assertEquals(listOffsetsRequestKeys.size, numResponse)
  }

  @Test
  def testResponseOnPartialError(): Unit = {
    var numResponse = 0
    val responseCallback = (response: List[ListOffsetsTopicResponse]) => {
      response.foreach { topic =>
        topic.partitions().forEach { partition =>
          if (topic.name().equals("test1")) {
            assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), partition.errorCode())
            assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partition.timestamp())
            assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partition.offset())
            assertEquals(-1, partition.leaderEpoch())
          } else {
            assertEquals(Errors.NONE.code(), partition.errorCode())
            assertEquals(100L, partition.timestamp())
            assertEquals(100L, partition.offset())
            assertEquals(50, partition.leaderEpoch())
          }
          numResponse += 1
        }
      }
    }

    val timestampAndOffset = new TimestampAndOffset(100L, 100L, Optional.of(50))
    val taskFuture = new CompletableFuture[T]()
    taskFuture.complete(Right(Some(timestampAndOffset)))

    var cancelledCount = 0
    val jobFuture = mock(classOf[CompletableFuture[Void]])
    val holder: AsyncOffsetReadFutureHolder[T] = mock(classOf[AsyncOffsetReadFutureHolder[T]])
    when(holder.taskFuture).thenAnswer(_ => taskFuture)
    when(holder.jobFuture).thenReturn(jobFuture)
    when(jobFuture.cancel(anyBoolean())).thenAnswer(_ => {
      cancelledCount += 1
      true
    })

    val errorFutureHolder: AsyncOffsetReadFutureHolder[T] = mock(classOf[AsyncOffsetReadFutureHolder[T]])
    val errorTaskFuture = new CompletableFuture[T]()
    errorTaskFuture.complete(Left(new TimeoutException("Timed out!")))
    when(errorFutureHolder.taskFuture).thenAnswer(_ => errorTaskFuture)
    when(errorFutureHolder.jobFuture).thenReturn(jobFuture)

    val statusByPartition = mutable.Map(
      new TopicPartition("test", 0) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test", 1) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test1", 0) -> ListOffsetsPartitionStatus(None, Some(errorFutureHolder))
    )

    val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version = 5, statusByPartition, replicaManager, responseCallback)
    val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList.asJava
    purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys)

    assertEquals(0, cancelledCount)
    assertEquals(listOffsetsRequestKeys.size, numResponse)
  }

  @Test
  def testPartialResponseWhenNotLeaderOrFollowerExceptionOnOnePartition(): Unit = {
    var numResponse = 0
    val responseCallback = (response: List[ListOffsetsTopicResponse]) => {
      response.foreach { topic =>
        topic.partitions().forEach { partition =>
          if (topic.name().equals("test1") && partition.partitionIndex() == 0) {
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code(), partition.errorCode())
            assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partition.timestamp())
            assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partition.offset())
            assertEquals(-1, partition.leaderEpoch())
          } else {
            assertEquals(Errors.NONE.code(), partition.errorCode())
            assertEquals(100L, partition.timestamp())
            assertEquals(100L, partition.offset())
            assertEquals(50, partition.leaderEpoch())
          }
          numResponse += 1
        }
      }
    }

    val timestampAndOffset = new TimestampAndOffset(100L, 100L, Optional.of(50))
    val taskFuture = new CompletableFuture[T]()
    taskFuture.complete(Right(Some(timestampAndOffset)))

    var cancelledCount = 0
    val jobFuture = mock(classOf[CompletableFuture[Void]])
    val holder: AsyncOffsetReadFutureHolder[T] = mock(classOf[AsyncOffsetReadFutureHolder[T]])
    when(holder.taskFuture).thenAnswer(_ => taskFuture)
    when(holder.jobFuture).thenReturn(jobFuture)
    when(jobFuture.cancel(anyBoolean())).thenAnswer(_ => {
      cancelledCount += 1
      true
    })

    when(replicaManager.getPartitionOrException(new TopicPartition("test1", 0)))
      .thenThrow(new NotLeaderOrFollowerException("Not leader or follower!"))
    val errorFutureHolder: AsyncOffsetReadFutureHolder[T] = mock(classOf[AsyncOffsetReadFutureHolder[T]])
    val errorTaskFuture = new CompletableFuture[T]()
    when(errorFutureHolder.taskFuture).thenAnswer(_ => errorTaskFuture)
    when(errorFutureHolder.jobFuture).thenReturn(jobFuture)

    val statusByPartition = mutable.Map(
      new TopicPartition("test", 0) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test", 1) -> ListOffsetsPartitionStatus(None, Some(holder)),
      new TopicPartition("test1", 0) -> ListOffsetsPartitionStatus(None, Some(errorFutureHolder)),
      new TopicPartition("test1", 1) -> ListOffsetsPartitionStatus(None, Some(holder))
    )

    val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version = 5, statusByPartition, replicaManager, responseCallback)
    val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList.asJava
    purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys)

    assertEquals(1, cancelledCount)
    assertEquals(listOffsetsRequestKeys.size, numResponse)
  }
}
