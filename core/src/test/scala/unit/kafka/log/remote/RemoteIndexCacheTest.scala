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

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.UUID

import kafka.log.{OffsetIndex, OffsetPosition, TimeIndex}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}

class RemoteIndexCacheTest {

  val rlsm: org.apache.kafka.common.log.remote.storage.RemoteStorageManager = EasyMock.createMock(classOf[org.apache.kafka.common.log.remote.storage.RemoteStorageManager])
  var rlsMetadata: RemoteLogSegmentMetadata = _
  var cache: RemoteIndexCache = _
  var offsetIndex: OffsetIndex = _
  var timeIndex: TimeIndex = _
  val maxEntries = 30
  val baseOffset = 45L

  @Before
  def setup(): Unit = {
    offsetIndex = new OffsetIndex(createTempFile(), baseOffset, maxIndexSize = maxEntries * 8)
    timeIndex = new TimeIndex(createTempFile(), baseOffset = baseOffset, maxIndexSize = maxEntries * 12)

    appendIndexEntries()

    // fetch indexes only once to build the cache, later it should be available in the cache
    EasyMock.expect(rlsm.fetchOffsetIndex(EasyMock.anyObject(classOf[RemoteLogSegmentMetadata])))
      .andReturn(new FileInputStream(offsetIndex.file))
      .times(1)
    EasyMock.expect(rlsm.fetchTimestampIndex(EasyMock.anyObject(classOf[RemoteLogSegmentMetadata])))
      .andReturn(new FileInputStream(timeIndex.file))
      .times(1)
    EasyMock.replay(rlsm)

    val logDir = Files.createTempDirectory("kafka-").toString
    cache = new RemoteIndexCache(remoteStorageManager = rlsm, logDir = logDir)

    rlsMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(new TopicPartition("foo", 0),
      UUID.randomUUID()), baseOffset, offsetIndex.lastOffset, -1L, 1, "foo".getBytes())

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

  private def assertIndexEntries(): Unit = {
    for (i <- 0 until offsetIndex.maxEntries)
      assertEquals(OffsetPosition(offsetIndex.baseOffset + i + 1, i), offsetIndex.entry(i))
    assertTrue(timeIndex.entries > 0)
    assertTrue(timeIndex.file.exists())
  }

  def createTempFile(): File = {
    val file = File.createTempFile("kafka-test-", ".tmp")
    Files.delete(file.toPath)
    file
  }

  @After
  def cleanup(): Unit = {
    EasyMock.reset(rlsm)

    if (offsetIndex != null) offsetIndex.deleteIfExists()
    if (timeIndex != null) timeIndex.deleteIfExists()
    cache.close()
  }

  @Test
  def testLoadingIndexFromRemoteStorage(): Unit = {

    assertIndexEntries()

    val offsetPosition1 = offsetIndex.entry(1)
    // this call should have invoked fetchOffsetIndex, fetchTimestampIndex once
    val resultOffsetPosition1 = cache.lookupOffset(rlsMetadata, offsetPosition1.offset)
    assertEquals(offsetPosition1, resultOffsetPosition1)

    // this should not cause fetching index from RemoteLogStorageManager as it is already fetched earlier
    // this is checked by setting expectation times as 1 on the mock
    val offsetPosition2 = offsetIndex.entry(2)
    val resultOffsetPosition2 = cache.lookupOffset(rlsMetadata, offsetPosition2.offset)
    assertEquals(offsetPosition2, resultOffsetPosition2)
  }

  @Test
  def testPositionForNonExistingIndexFromRemoteStorage(): Unit = {
    // offsets beyond this
    val lastOffsetPosition = cache.lookupOffset(rlsMetadata, offsetIndex.lastOffset)
    val greaterOffsetThanLastOffset = offsetIndex.lastOffset + 1
    val resultOffsetPosition1 = cache.lookupOffset(rlsMetadata, greaterOffsetThanLastOffset)
    assertEquals(lastOffsetPosition, resultOffsetPosition1)

    val nonExistentOffsetPosition = OffsetPosition(baseOffset, 0)
    val lowerOffsetThanBaseOffset = offsetIndex.baseOffset - 1
    val resultOffsetPosition2 = cache.lookupOffset(rlsMetadata, lowerOffsetThanBaseOffset)
    assertEquals(nonExistentOffsetPosition, resultOffsetPosition2)
  }
}
