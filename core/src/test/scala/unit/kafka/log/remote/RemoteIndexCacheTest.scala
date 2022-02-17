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

import kafka.log.{OffsetIndex, OffsetPosition, TimeIndex}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.mockito.{ArgumentMatchers, Mockito}

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.Collections

class RemoteIndexCacheTest {

  val rsm: RemoteStorageManager = Mockito.mock(classOf[RemoteStorageManager])
  var rlsMetadata: RemoteLogSegmentMetadata = _
  var cache: RemoteIndexCache = _
  var offsetIndex: OffsetIndex = _
  var timeIndex: TimeIndex = _
  val maxEntries = 30
  val baseOffset = 45L

  @BeforeEach
  def setup(): Unit = {
    val tempDir = Files.createTempDirectory("kafka-test-").toFile
    tempDir.deleteOnExit()

    offsetIndex = new OffsetIndex(new File(tempDir, "offset-index"), baseOffset, maxIndexSize = maxEntries * 8)
    timeIndex = new TimeIndex(new File(tempDir, "time-index"), baseOffset = baseOffset, maxIndexSize = maxEntries * 12)
    appendIndexEntries()

    // Fetch indexes only once to build the cache, later it should be available in the cache
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.OFFSET)))
      .thenReturn(new FileInputStream(offsetIndex.file))

    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.TIMESTAMP)))
      .thenReturn(new FileInputStream(timeIndex.file))

    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.TRANSACTION)))
      .thenReturn(new FileInputStream(File.createTempFile("kafka-test-", ".txn", tempDir)))

    val logDir = Files.createTempDirectory("kafka-").toString
    cache = new RemoteIndexCache(remoteStorageManager = rsm, logDir = logDir)

    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    rlsMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      baseOffset, offsetIndex.lastOffset, -1L, 1, 1024, 1024,
      Collections.singletonMap(0, 0L))
  }

  private def appendIndexEntries(): Unit = {
    val curTime = System.currentTimeMillis()
    for (i <- 0 until offsetIndex.maxEntries) {
      val offset = offsetIndex.baseOffset + i + 1
      offsetIndex.append(offset, i)
      timeIndex.maybeAppend(curTime + i, offset, skipFullCheck = true)
    }

    offsetIndex.flush()
    timeIndex.flush()
  }

  @AfterEach
  def cleanup(): Unit = {
    Mockito.reset(rsm)

    if (offsetIndex != null) offsetIndex.deleteIfExists()
    if (timeIndex != null) timeIndex.deleteIfExists()
    cache.close()
  }

  @Test
  def testLoadingIndexFromRemoteStorage(): Unit = {
    val offsetPosition1 = offsetIndex.entry(1)
    // this call should have invoked fetchOffsetIndex, fetchTimestampIndex once
    val resultOffset = cache.lookupOffset(rlsMetadata, offsetPosition1.offset)
    assertEquals(offsetPosition1.position, resultOffset)
    verify(rsm, times(1)).fetchIndex(rlsMetadata, IndexType.OFFSET)
    verify(rsm, times(1)).fetchIndex(rlsMetadata, IndexType.TIMESTAMP)

    // this should not cause fetching index from RemoteStorageManager as it is already fetched earlier
    // this is checked by setting expectation times as 1 on the mock
    val offsetPosition2 = offsetIndex.entry(2)
    val resultOffset2 = cache.lookupOffset(rlsMetadata, offsetPosition2.offset)
    assertEquals(offsetPosition2.position, resultOffset2)
    assertNotNull(cache.getIndexEntry(rlsMetadata))

    verify(rsm, times(1)).fetchIndex(rlsMetadata, IndexType.OFFSET)
    verify(rsm, times(1)).fetchIndex(rlsMetadata, IndexType.TIMESTAMP)
  }

  @Test
  def testPositionForNonExistingIndexFromRemoteStorage(): Unit = {

    val lastOffsetPosition = cache.lookupOffset(rlsMetadata, offsetIndex.lastOffset)
    val greaterOffsetThanLastOffset = offsetIndex.lastOffset + 1
    assertEquals(lastOffsetPosition, cache.lookupOffset(rlsMetadata, greaterOffsetThanLastOffset))

    // offsetIndex.lookup() returns OffsetPosition(baseOffset, 0) for offsets smaller than least entry in the offset index.
    val nonExistentOffsetPosition = OffsetPosition(baseOffset, 0)
    val lowerOffsetThanBaseOffset = offsetIndex.baseOffset - 1
    assertEquals(nonExistentOffsetPosition.position, cache.lookupOffset(rlsMetadata, lowerOffsetThanBaseOffset))
  }
}
