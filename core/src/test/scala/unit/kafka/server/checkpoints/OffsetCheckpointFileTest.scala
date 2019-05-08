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
import org.junit.Assert._
import org.junit.Test

import scala.collection.Map

class OffsetCheckpointFileTest extends Logging {

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

  @Test(expected = classOf[KafkaStorageException])
  def shouldThrowIfVersionIsNotRecognised(): Unit = {
    val file = TestUtils.tempFile()
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val checkpointFile = new CheckpointFile(file, OffsetCheckpointFile.CurrentVersion + 1,
      OffsetCheckpointFile.Formatter, logDirFailureChannel, file.getParent)
    checkpointFile.write(Seq(new TopicPartition("foo", 5) -> 10L))
    new OffsetCheckpointFile(checkpointFile.file, logDirFailureChannel).read()
  }

}
