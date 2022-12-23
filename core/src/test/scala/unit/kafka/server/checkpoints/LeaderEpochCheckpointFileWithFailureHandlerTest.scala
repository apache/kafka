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
import org.apache.kafka.server.log.internals.{EpochEntry, LeaderEpochCheckpointFile}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class LeaderEpochCheckpointFileWithFailureHandlerTest extends Logging {

  @Test
  def shouldPersistAndOverwriteAndReloadFile(): Unit ={
    val file = TestUtils.tempFile("temp-checkpoint-file", System.nanoTime().toString)

    val checkpoint = new LeaderEpochCheckpointFile(file)

    //Given
    val epochs = Seq(new EpochEntry(0, 1L), new EpochEntry(1, 2L), new EpochEntry(2, 3L))

    //When
    checkpoint.write(epochs.asJava)

    //Then
    assertEquals(epochs, checkpoint.read())

    //Given overwrite
    val epochs2 = Seq(new EpochEntry(3, 4L), new EpochEntry(4, 5L))

    //When
    checkpoint.write(epochs2.asJava)

    //Then
    assertEquals(epochs2, checkpoint.read().asScala.toSeq)
  }

  @Test
  def shouldRetainValuesEvenIfCheckpointIsRecreated(): Unit ={
    val file = TestUtils.tempFile("temp-checkpoint-file", System.nanoTime().toString)

    //Given a file with data in
    val checkpoint = new LeaderEpochCheckpointFile(file)
    val epochs = Seq(new EpochEntry(0, 1L), new EpochEntry(1, 2L), new EpochEntry(2, 3L))
    checkpoint.write(epochs.asJava)

    //When we recreate
    val checkpoint2 = new LeaderEpochCheckpointFile(file)

    //The data should still be there
    assertEquals(epochs, checkpoint2.read().asScala.toSeq)
  }
}
