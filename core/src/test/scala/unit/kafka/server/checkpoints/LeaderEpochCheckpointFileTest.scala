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

import kafka.server.epoch.EpochEntry
import kafka.server.{GlobalConfig, LogDirFailureChannel}
import kafka.utils.Logging
import org.apache.kafka.common.errors.KafkaStorageException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.nio.file.Files

class LeaderEpochCheckpointFileTest extends Logging {

  @Test
  def shouldPersistAndOverwriteAndReloadFile(): Unit ={
    val file = File.createTempFile("temp-checkpoint-file", System.nanoTime().toString)
    file.deleteOnExit()

    val checkpoint = new LeaderEpochCheckpointFile(file)

    //Given
    val epochs = Seq(EpochEntry(0, 1L), EpochEntry(1, 2L), EpochEntry(2, 3L))

    //When
    checkpoint.write(epochs)

    //Then
    assertEquals(epochs, checkpoint.read())

    //Given overwrite
    val epochs2 = Seq(EpochEntry(3, 4L), EpochEntry(4, 5L))

    //When
    checkpoint.write(epochs2)

    //Then
    assertEquals(epochs2, checkpoint.read())
  }

  @Test
  def shouldRetainValuesEvenIfCheckpointIsRecreated(): Unit ={
    val file = File.createTempFile("temp-checkpoint-file", System.nanoTime().toString)
    file.deleteOnExit()

    //Given a file with data in
    val checkpoint = new LeaderEpochCheckpointFile(file)
    val epochs = Seq(EpochEntry(0, 1L), EpochEntry(1, 2L), EpochEntry(2, 3L))
    checkpoint.write(epochs)

    //When we recreate
    val checkpoint2 = new LeaderEpochCheckpointFile(file)

    //The data should still be there
    assertEquals(epochs, checkpoint2.read())
  }


  @ParameterizedTest
  @ValueSource(booleans =  Array(true, false))
  def testCheckpointFileWithCorruptedVersion(liDropCorruptedFilesEnable: Boolean): Unit = {
    testCheckpointFileWithCorruption(liDropCorruptedFilesEnable, bw => {
      bw.write("100")
      bw.newLine()
    })
  }

  @ParameterizedTest
  @ValueSource(booleans =  Array(true, false))
  def testCheckpointFileWithIncorrectNumberOfEntries(liDropCorruptedFilesEnable: Boolean): Unit = {
    testCheckpointFileWithCorruption(liDropCorruptedFilesEnable, bw => {
      bw.write(0.toString)
      bw.newLine()

      bw.write(3.toString)
      bw.newLine()
    })
  }

  @ParameterizedTest
  @ValueSource(booleans =  Array(true, false))
  def testCheckpointFileWithCorruptedEntries(liDropCorruptedFilesEnable: Boolean): Unit = {
    testCheckpointFileWithCorruption(liDropCorruptedFilesEnable, bw => {
      bw.write(0.toString)
      bw.newLine()

      bw.write(1.toString)
      bw.newLine()

      val epoch = 100
      bw.write(epoch.toString) // this line is malformed since there is only epoch without the starting offset
    })
  }

  @ParameterizedTest
  @ValueSource(booleans =  Array(true, false))
  def testCheckpointFileWithNumberFormatExceptions(liDropCorruptedFilesEnable: Boolean): Unit = {
    testCheckpointFileWithCorruption(liDropCorruptedFilesEnable, bw => {
      bw.write(0.toString)
      bw.newLine()

      bw.write(1.toString)
      bw.newLine()

      bw.write("not-a-number 3") // this line is malformed since the first field cannot be converted to a number
    })
  }

  private def testCheckpointFileWithCorruption(liDropCorruptedFilesEnable: Boolean, testFunc: BufferedWriter => Unit): Unit = {
    GlobalConfig.liDropCorruptedFilesEnable = liDropCorruptedFilesEnable
    val file = File.createTempFile("temp-checkpoint-file", System.nanoTime().toString)
    file.deleteOnExit()

    // create a file with corrupted version number
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    testFunc(bw)
    bw.close()

    val checkpoint = new LeaderEpochCheckpointFile(file, new LogDirFailureChannel(1))
    if (liDropCorruptedFilesEnable) {
      val epochs = checkpoint.read()
      assertTrue(epochs.isEmpty, "reading from a corrupted leader-epoch-checkpoint file should have returned an empty list of EpochEntries")
      // verify that the file no longer exists
      assertTrue(file.exists(), "the file should still exists")
      assertEquals(0, Files.size(file.toPath), "the content of the corrupted file should have been cleared")
    } else {
      assertThrows(classOf[KafkaStorageException], ()=> checkpoint.read())
    }
  }
}
