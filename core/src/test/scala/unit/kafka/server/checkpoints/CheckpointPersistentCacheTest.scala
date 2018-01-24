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
package unit.kafka.server.checkpoints

import kafka.server.checkpoints.{CheckpointPersistentCacheFile, OffsetCheckpointFile}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class CheckpointPersistentCacheTest extends JUnitSuite with Logging {
  @Test
  def shouldPersistAndOverwriteAndReloadFile(): Unit = {
    val checkpointFile = TestUtils.tempFile()
    checkpointFile.deleteOnExit()

    val checkpointStore = new CheckpointPersistentCacheFile[TopicPartition, Long](
      checkpointFile,
      OffsetCheckpointFile.Formatter)

    val tp = new TopicPartition("topic1", 0)

    checkpointStore.update(Map(tp -> 55L, new TopicPartition("topic2", 234) -> 19242342458L))
    checkpointStore.persist()

    assertEquals(55L, checkpointStore.getCheckpoint(tp).get)

    // A new instance should read the cache off disk.
    val checkpointStore2 = new CheckpointPersistentCacheFile[TopicPartition, Long](
      checkpointFile,
      OffsetCheckpointFile.Formatter)

    assertEquals(55L, checkpointStore2.getCheckpoint(tp).get)
  }
}
