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

import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

import scala.collection.Map

class OffsetAndTimesCheckpointFileTest extends Logging {

  @Test
  def shouldPersistAndOverwriteOffsetsForPartition(): Unit = {
    val assignedPartition = new TopicPartition("test", 0)
    val checkpoint = new OffsetAndTimesCheckpointFile(TestUtils.tempFile(), assignedPartition)

    val offset = new OffsetAndTimestamp(1000, 1000)
    checkpoint.write(offset)
    assertEquals(offset, checkpoint.read()(assignedPartition))

    val newOffset = new OffsetAndTimestamp(1500, 1500)
    checkpoint.write(newOffset)
    assertEquals(newOffset, checkpoint.read()(assignedPartition))
  }

  @Test
  def shouldReturnEmptyMapForEmptyFile(): Unit = {
    val checkpoint = new OffsetAndTimesCheckpointFile(TestUtils.tempFile(), new TopicPartition("dummy", 0))
    assertEquals(Map(), checkpoint.read())

    checkpoint.write(null)
    assertEquals(Map(), checkpoint.read())
  }

}