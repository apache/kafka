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

package kafka.log

import java.io.File
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{DuplicateSequenceNumberException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerIdMappingTest extends JUnitSuite {
  var idMappingDir: File = null
  var config: LogConfig = null
  var idMapping: ProducerIdMapping = null
  val partition = new TopicPartition("test", 0)
  val pid = 1L
  val maxPidExpirationMs = 60 * 1000
  val time = new MockTime

  @Before
  def setUp(): Unit = {
    // Create configuration including number of snapshots to hold
    val props = new Properties()
    config = LogConfig(props)

    // Create temporary directory
    idMappingDir = TestUtils.tempDir()

    // Instantiate IdMapping
    idMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
  }

  @After
  def tearDown(): Unit = {
    idMappingDir.listFiles().foreach(f => f.delete())
    idMappingDir.deleteOnExit()
  }

  @Test
  def testBasicIdMapping(): Unit = {
    val epoch = 0.toShort

    // First entry for id 0 added
    checkAndUpdate(idMapping, pid, 0, epoch, 0L, 0L)

    // Second entry for id 0 added
    checkAndUpdate(idMapping, pid, 1, epoch, 0L, 1L)

    // Duplicate sequence number (matches previous sequence number)
    assertThrows[DuplicateSequenceNumberException] {
      checkAndUpdate(idMapping, pid, 1, epoch, 0L, 1L)
    }

    // Invalid sequence number (greater than next expected sequence number)
    assertThrows[OutOfOrderSequenceException] {
      checkAndUpdate(idMapping, pid, 5, epoch, 0L, 2L)
    }

    // Change epoch
    checkAndUpdate(idMapping, pid, 0, (epoch + 1).toShort, 0L, 3L)

    // Incorrect epoch
    assertThrows[ProducerFencedException] {
      checkAndUpdate(idMapping, pid, 0, epoch, 0L, 4L)
    }
  }

  @Test
  def testTakeSnapshot(): Unit = {
    val epoch = 0.toShort
    checkAndUpdate(idMapping, pid, 0, epoch, 0L, 0L)
    checkAndUpdate(idMapping, pid, 1, epoch, 1L, 1L)

    // Take snapshot
    idMapping.maybeTakeSnapshot()

    // Check that file exists and it is not empty
    assertEquals("Directory doesn't contain a single file as expected", 1, idMappingDir.list().length)
    assertTrue("Snapshot file is empty", idMappingDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    val epoch = 0.toShort
    checkAndUpdate(idMapping, pid, 0, epoch, 0L, time.milliseconds)
    checkAndUpdate(idMapping, pid, 1, epoch, 1L, time.milliseconds)
    idMapping.maybeTakeSnapshot()
    val recoveredMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(3L, time.milliseconds)

    // entry added after recovery
    checkAndUpdate(recoveredMapping, pid, 2, epoch, 2L, time.milliseconds)
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testRemoveExpiredPidsOnReload(): Unit = {
    val epoch = 0.toShort
    checkAndUpdate(idMapping, pid, 0, epoch, 0L, 0)
    checkAndUpdate(idMapping, pid, 1, epoch, 1L, 1)

    idMapping.maybeTakeSnapshot()
    val recoveredMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(1L, 70000)

    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Hence
    // we should get an out of order sequence exception.
    checkAndUpdate(recoveredMapping, pid, 2, epoch, 2L, 70001)
  }


  @Test
  def testRemoveOldSnapshot(): Unit = {
    val epoch = 0.toShort
    checkAndUpdate(idMapping, pid, 0, epoch, 0L, 1L)
    checkAndUpdate(idMapping, pid, 1, epoch, 1L, 2L)

    idMapping.maybeTakeSnapshot()

    checkAndUpdate(idMapping, pid, 2, epoch, 2L, 3L)

    idMapping.maybeTakeSnapshot()

    assertEquals(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
               1, idMappingDir.listFiles().length)
  }

  @Test
  def testSkipSnapshotIfOffsetUnchanged(): Unit = {
    val epoch = 0.toShort
    checkAndUpdate(idMapping, pid, 0, epoch, 0L, 0L)

    idMapping.maybeTakeSnapshot()

    // nothing changed so there should be no new snapshot
    idMapping.maybeTakeSnapshot()

    assertEquals(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
      1, idMappingDir.listFiles().length)
  }

  @Test
  def testStartOffset(): Unit = {
    val epoch = 0.toShort
    val pid2 = 2L
    checkAndUpdate(idMapping, pid2, 0, epoch, 0L, 1L)
    checkAndUpdate(idMapping, pid, 0, epoch, 1L, 2L)
    checkAndUpdate(idMapping, pid, 1, epoch, 2L, 3L)
    checkAndUpdate(idMapping, pid, 2, epoch, 3L, 4L)
    idMapping.maybeTakeSnapshot()

    intercept[OutOfOrderSequenceException] {
      val recoveredMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
      recoveredMapping.truncateAndReload(1L, time.milliseconds)
      checkAndUpdate(recoveredMapping, pid2, 1, epoch, 4L, 5L)
    }
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testPidExpirationTimeout() {
    val epoch = 5.toShort
    val sequence = 37
    checkAndUpdate(idMapping, pid, sequence, epoch, 1L)
    time.sleep(maxPidExpirationMs + 1)
    idMapping.checkForExpiredPids(time.milliseconds)
    checkAndUpdate(idMapping, pid, sequence + 1, epoch, 1L)
  }

  @Test
  def testLoadPid() {
    val epoch = 5.toShort
    val sequence = 37
    val createTimeMs = time.milliseconds
    idMapping.load(pid, ProducerIdEntry(epoch, sequence, 0L, 1, createTimeMs), time.milliseconds)
    checkAndUpdate(idMapping, pid, sequence + 1, epoch, 2L)
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testLoadIgnoresExpiredPids() {
    val epoch = 5.toShort
    val sequence = 37

    val createTimeMs = time.milliseconds
    time.sleep(maxPidExpirationMs + 1)
    val loadTimeMs = time.milliseconds
    idMapping.load(pid, ProducerIdEntry(epoch, sequence, 0L, 1, createTimeMs), loadTimeMs)

    // entry wasn't loaded, so this should fail
    checkAndUpdate(idMapping, pid, sequence + 1, epoch, 2L)
  }

  private def checkAndUpdate(mapping: ProducerIdMapping,
                             pid: Long,
                             seq: Int,
                             epoch: Short,
                             lastOffset: Long,
                             timestamp: Long = time.milliseconds()): Unit = {
    val numRecords = 1
    val incomingPidEntry = ProducerIdEntry(epoch, seq, lastOffset, numRecords, timestamp)
    val producerAppendInfo = new ProducerAppendInfo(pid, mapping.lastEntry(pid).getOrElse(ProducerIdEntry.Empty))
    producerAppendInfo.append(incomingPidEntry)
    mapping.update(producerAppendInfo)
  }

}
