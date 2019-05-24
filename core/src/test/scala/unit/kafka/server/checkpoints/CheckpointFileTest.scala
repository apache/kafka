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

package unit.kafka.server.checkpoints

import java.io.File
import java.util.UUID

import kafka.server.LogDirFailureChannel
import kafka.server.checkpoints.{CheckpointFile, OffsetCheckpointFileEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class CheckpointFileTest {
  @Test
  def testRoundTripCheckpoints(): Unit = {
    val file = File.createTempFile("temp-checkpoint-file", UUID.randomUUID().toString)
    file.deleteOnExit()
    val failureChannel = new LogDirFailureChannel(1)
    val checkpointFile = CheckpointFile(file, 0, failureChannel, file.getParent, OffsetCheckpointFileEntry.apply)
    val entries = Seq(
      new OffsetCheckpointFileEntry(new TopicPartition("foo", 0), 0L),
      new OffsetCheckpointFileEntry(new TopicPartition("foo", 1), 1L),
      new OffsetCheckpointFileEntry(new TopicPartition("foo", 2), 2L))
    checkpointFile.write(entries)
    val readEntries = checkpointFile.read()
    for (entry <- readEntries) {
      Assert.assertEquals(entry.topicPartition.topic(), "foo")
      Assert.assertEquals(entry.topicPartition.partition(), entry.offset)
    }
  }

  @Test
  def testReadDeletedFile(): Unit = {
    val file = File.createTempFile("temp-checkpoint-file", UUID.randomUUID().toString)
    file.deleteOnExit()
    val failureChannel = new LogDirFailureChannel(1)
    val checkpointFile = CheckpointFile(file, 0, failureChannel, file.getParent, OffsetCheckpointFileEntry.apply)
    file.delete()
    Assert.assertThrows(
      "Expected read of deleted file to throw KafkaStorageException",
      classOf[KafkaStorageException], new ThrowingRunnable {
        override def run(): Unit = {
          checkpointFile.read()
        }
      })
    Assert.assertEquals("Expected checkpoint logdir to be failed", failureChannel.takeNextOfflineLogDir(), file.getParent)
  }
}
