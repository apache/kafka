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

import org.apache.kafka.storage.internals.checkpoint.InMemoryLeaderEpochCheckpoint
import org.apache.kafka.storage.internals.log.EpochEntry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

class InMemoryLeaderEpochCheckpointTest {

  @Test
  def shouldAppendNewEntry(): Unit = {
    val checkpoint = new InMemoryLeaderEpochCheckpoint()
    val epochs = java.util.Arrays.asList(new EpochEntry(0, 1L), new EpochEntry(1, 2L), new EpochEntry(2, 3L))
    checkpoint.write(epochs)
    assertEquals(epochs, checkpoint.read())

    val epochs2 = java.util.Arrays.asList(new EpochEntry(3, 4L), new EpochEntry(4, 5L))
    checkpoint.write(epochs2)

    assertEquals(epochs2, checkpoint.read())
  }

  @Test
  def testReadAsByteBuffer(): Unit = {
    val checkpoint = new InMemoryLeaderEpochCheckpoint()
    val expectedEpoch = 0
    val expectedStartOffset = 1L
    val expectedVersion = 0
    val epochs = java.util.Arrays.asList(new EpochEntry(expectedEpoch, expectedStartOffset))
    checkpoint.write(epochs)
    assertEquals(epochs, checkpoint.read())
    val buffer = checkpoint.readAsByteBuffer()

    val bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.array()), StandardCharsets.UTF_8))
    assertEquals(expectedVersion.toString, bufferedReader.readLine())
    assertEquals(epochs.size().toString, bufferedReader.readLine())
    assertEquals(s"$expectedEpoch $expectedStartOffset", bufferedReader.readLine())
  }
}
