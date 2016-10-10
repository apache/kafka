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

package unit.kafka.idempotence

import java.io.File
import java.util.Properties

import kafka.log.{LogConfig, ProducerIdMapping}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{DuplicateSequenceNumberException, InvalidSequenceNumberException, ProducerFencedException}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerIdMappingTest extends JUnitSuite {
  var idMappingDir: File = null
  var config: LogConfig = null
  var idMapping: ProducerIdMapping = null
  val partition = new TopicPartition("test", 0)
  val pid = 1L

  @Before
  def setUp(): Unit = {
    // Create configuration including number of snapshots to hold
    val props = new Properties()
    props.setProperty(LogConfig.MaxIdMapSnapshotsProp, "1")
    config = LogConfig(props)

    // Create temporary directory
    idMappingDir = TestUtils.tempDir()

    // Instantiate IdMapping
    idMapping = new ProducerIdMapping(partition, config, idMappingDir)
  }

  @After
  def tearDown(): Unit = {
    idMappingDir.listFiles().foreach(f => f.delete())
    idMappingDir.deleteOnExit()
  }

  @Test
  def testBasicIdMapping(): Unit = {
    // First entry for id 0 added
    checkAndUpdate(idMapping, pid, 0, 0, 0L)

    // Second entry for id 0 added
    checkAndUpdate(idMapping, pid, 1, 0, 0L)

    // Duplicate sequence number (matches previous sequence number)
    assertThrows[DuplicateSequenceNumberException] {
      checkAndUpdate(idMapping, pid, 1, 0, 0L)
    }

    // Invalid sequence number (greater than next expected sequence number)
    assertThrows[InvalidSequenceNumberException] {
      checkAndUpdate(idMapping, pid, 5, 0, 0L)
    }

    // Change epoch
    checkAndUpdate(idMapping, pid, 0, 1, 0L)

    // Incorrect epoch
    assertThrows[ProducerFencedException] {
      checkAndUpdate(idMapping, pid, 0, 0, 0L)
    }
  }

  @Test
  def testTakeSnapshot(): Unit = {
    checkAndUpdate(idMapping, pid, 0, 0, 0L)
    checkAndUpdate(idMapping, pid, 1, 0, 1L)

    // Take snapshot
    idMapping.maybeTakeSnapshot()

    // Check that file exists and it is not empty
    assertEquals("Directory doesn't contain a single file as expected", 1, idMappingDir.list().length)
    assertTrue("Snapshot file is empty", idMappingDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    checkAndUpdate(idMapping, pid, 0, 0, 0L)
    checkAndUpdate(idMapping, pid, 1, 0, 1L)

    idMapping.maybeTakeSnapshot()
    val recoveredMapping = new ProducerIdMapping(partition, config, idMappingDir)
    recoveredMapping.truncateAndReload(1L)

    // entry added after recovery
    checkAndUpdate(recoveredMapping, pid, 2, 0, 2L)
  }

  @Test
  def testRemoveOldSnapshot(): Unit = {
    checkAndUpdate(idMapping, pid, 0, 0, 0L)
    checkAndUpdate(idMapping, pid, 1, 0, 1L)

    idMapping.maybeTakeSnapshot()

    checkAndUpdate(idMapping, pid, 2, 0, 2L)

    idMapping.maybeTakeSnapshot()

    assertEquals(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
               1, idMappingDir.listFiles().length)
  }

  @Test
  def testSkipSnapshotIfOffsetUnchanged(): Unit = {
    checkAndUpdate(idMapping, pid, 0, 0, 0L)

    idMapping.maybeTakeSnapshot()

    // nothing changed so there should be no new snapshot
    idMapping.maybeTakeSnapshot()

    assertEquals(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
      1, idMappingDir.listFiles().length)
  }

  @Test
  def testStartOffset(): Unit = {
    val pid2 = 2L
    checkAndUpdate(idMapping, pid2, 0, 0, 0L)
    checkAndUpdate(idMapping, pid, 0, 0, 1L)
    checkAndUpdate(idMapping, pid, 1, 0, 2L)
    checkAndUpdate(idMapping, pid, 2, 0, 3L)
    idMapping.maybeTakeSnapshot()

    intercept[InvalidSequenceNumberException] {
      val recoveredMapping = new ProducerIdMapping(partition, config, idMappingDir)
      recoveredMapping.truncateAndReload(1L)
      checkAndUpdate(recoveredMapping, pid2, 1, 0, 4L)
    }
  }

  private def checkAndUpdate(mapping: ProducerIdMapping,
                             pid: Long,
                             seq: Int,
                             epoch: Short,
                             lastOffset: Long): Unit = {
    checkAndUpdate(mapping, pid, seq, seq, epoch, lastOffset)
  }

  private def checkAndUpdate(mapping: ProducerIdMapping,
                             pid: Long,
                             firstSeq: Int,
                             lastSeq: Int,
                             epoch: Short,
                             lastOffset: Long): Unit = {
    mapping.checkSeqAndEpoch(pid, firstSeq, epoch)
    mapping.update(pid, lastSeq, epoch, lastOffset)
  }
}
