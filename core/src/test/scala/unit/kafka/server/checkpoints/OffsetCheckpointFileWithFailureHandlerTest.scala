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
package kafka.server.checkpoints

import kafka.server.LogDirFailureChannel
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito

import scala.collection.Map

class OffsetCheckpointFileWithFailureHandlerTest extends Logging {

  @Test
  def shouldPersistAndOverwriteAndReloadFile(): Unit = {

    val checkpoint = new OffsetCheckpointFile(TestUtils.tempFile())

    //Given
    val offsets = Map(new TopicPartition("foo", 1) -> 5L, new TopicPartition("bar", 2) -> 10L)

    //When
    checkpoint.write(offsets)

    //Then
    assertEquals(offsets, checkpoint.read())

    //Given overwrite
    val offsets2 = Map(new TopicPartition("foo", 2) -> 15L, new TopicPartition("bar", 3) -> 20L)

    //When
    checkpoint.write(offsets2)

    //Then
    assertEquals(offsets2, checkpoint.read())
  }

  @Test
  def shouldHandleMultipleLines(): Unit = {

    val checkpoint = new OffsetCheckpointFile(TestUtils.tempFile())

    //Given
    val offsets = Map(
      new TopicPartition("foo", 1) -> 5L, new TopicPartition("bar", 6) -> 10L,
      new TopicPartition("foo", 2) -> 5L, new TopicPartition("bar", 7) -> 10L,
      new TopicPartition("foo", 3) -> 5L, new TopicPartition("bar", 8) -> 10L,
      new TopicPartition("foo", 4) -> 5L, new TopicPartition("bar", 9) -> 10L,
      new TopicPartition("foo", 5) -> 5L, new TopicPartition("bar", 10) -> 10L
    )

    //When
    checkpoint.write(offsets)

    //Then
    assertEquals(offsets, checkpoint.read())
  }

  @Test
  def shouldReturnEmptyMapForEmptyFile(): Unit = {

    //When
    val checkpoint = new OffsetCheckpointFile(TestUtils.tempFile())

    //Then
    assertEquals(Map(), checkpoint.read())

    //When
    checkpoint.write(Map())

    //Then
    assertEquals(Map(), checkpoint.read())
  }

  @Test
  def shouldThrowIfVersionIsNotRecognised(): Unit = {
    val file = TestUtils.tempFile()
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val checkpointFile = new CheckpointFileWithFailureHandler(file, OffsetCheckpointFile.CurrentVersion + 1,
      OffsetCheckpointFile.Formatter, logDirFailureChannel, file.getParent)
    checkpointFile.write(Seq(new TopicPartition("foo", 5) -> 10L))
    assertThrows(classOf[KafkaStorageException], () => new OffsetCheckpointFile(checkpointFile.file, logDirFailureChannel).read())
  }

  @Test
  def testLazyOffsetCheckpoint(): Unit = {
    val logDir = "/tmp/kafka-logs"
    val mockCheckpointFile = Mockito.mock(classOf[OffsetCheckpointFile])

    val lazyCheckpoints = new LazyOffsetCheckpoints(Map(logDir -> mockCheckpointFile))
    Mockito.verify(mockCheckpointFile, Mockito.never()).read()

    val partition0 = new TopicPartition("foo", 0)
    val partition1 = new TopicPartition("foo", 1)
    val partition2 = new TopicPartition("foo", 2)

    Mockito.when(mockCheckpointFile.read()).thenReturn(Map(
      partition0 -> 1000L,
      partition1 -> 2000L
    ))

    assertEquals(Some(1000L), lazyCheckpoints.fetch(logDir, partition0))
    assertEquals(Some(2000L), lazyCheckpoints.fetch(logDir, partition1))
    assertEquals(None, lazyCheckpoints.fetch(logDir, partition2))

    Mockito.verify(mockCheckpointFile, Mockito.times(1)).read()
  }

  @Test
  def testLazyOffsetCheckpointFileInvalidLogDir(): Unit = {
    val logDir = "/tmp/kafka-logs"
    val mockCheckpointFile = Mockito.mock(classOf[OffsetCheckpointFile])
    val lazyCheckpoints = new LazyOffsetCheckpoints(Map(logDir -> mockCheckpointFile))
    assertThrows(classOf[IllegalArgumentException], () => lazyCheckpoints.fetch("/invalid/kafka-logs", new TopicPartition("foo", 0)))
  }

}
