/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{ByteArrayOutputStream, File}
import java.util.Properties

import kafka.log.{Log, LogConfig, LogManager}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.tools.DumpLogSegments.TimeIndexDumpErrors
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.fail

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class BatchInfo(records: Seq[SimpleRecord], hasKeys: Boolean, hasValues: Boolean)

class DumpLogSegmentsTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val segmentName = "00000000000000000000"
  val logFilePath = s"$logDir/$segmentName.log"
  val indexFilePath = s"$logDir/$segmentName.index"
  val timeIndexFilePath = s"$logDir/$segmentName.timeindex"
  val time = new MockTime(0, 0)

  val batches = new ArrayBuffer[BatchInfo]
  var log: Log = _

  @Before
  def setUp(): Unit = {
    val props = new Properties
    props.setProperty(LogConfig.IndexIntervalBytesProp, "128")
    log = Log(logDir, LogConfig(props), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

    val now = System.currentTimeMillis()
    val firstBatchRecords = (0 until 10).map { i => new SimpleRecord(now + i * 2, s"message key $i".getBytes, s"message value $i".getBytes)}
    batches += BatchInfo(firstBatchRecords, true, true)
    val secondBatchRecords = (10 until 30).map { i => new SimpleRecord(now + i * 3, s"message key $i".getBytes, null)}
    batches += BatchInfo(secondBatchRecords, true, false)
    val thirdBatchRecords = (30 until 50).map { i => new SimpleRecord(now + i * 5, null, s"message value $i".getBytes)}
    batches += BatchInfo(thirdBatchRecords, false, true)
    val fourthBatchRecords = (50 until 60).map { i => new SimpleRecord(now + i * 7, null)}
    batches += BatchInfo(fourthBatchRecords, false, false)

    batches.foreach { batchInfo =>
      log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0, batchInfo.records: _*),
        leaderEpoch = 0)
    }
    // Flush, but don't close so that the indexes are not trimmed and contain some zero entries
    log.flush()
  }

  @After
  def tearDown(): Unit = {
    log.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testPrintDataLog(): Unit = {

    def verifyRecordsInOutput(checkKeysAndValues: Boolean, args: Array[String]): Unit = {
      def isBatch(index: Int): Boolean = {
        var i = 0
        batches.zipWithIndex.foreach { case (batch, batchIndex) =>
          if (i == index)
            return true

          i += 1

          batch.records.indices.foreach { recordIndex =>
            if (i == index)
              return false
            i += 1
          }
        }
        fail(s"No match for index $index")
      }

      val output = runDumpLogSegments(args)
      val lines = output.split("\n")
      assertTrue(s"Data not printed: $output", lines.length > 2)
      val totalRecords = batches.map(_.records.size).sum
      var offset = 0
      val batchIterator = batches.iterator
      var batch : BatchInfo = null;
      (0 until totalRecords + batches.size).foreach { index =>
        val line = lines(lines.length - totalRecords - batches.size + index)
        // The base offset of the batch is the offset of the first record in the batch, so we
        // only increment the offset if it's not a batch
        if (isBatch(index)) {
          assertTrue(s"Not a valid batch-level message record: $line", line.startsWith(s"baseOffset: $offset lastOffset: "))
          batch = batchIterator.next()
        } else {
          assertTrue(s"Not a valid message record: $line", line.startsWith(s"${DumpLogSegments.RecordIndent} offset: $offset"))
          if (checkKeysAndValues) {
            var suffix = "headerKeys: []"
            if (batch.hasKeys)
              suffix += s" key: message key $offset"
            if (batch.hasValues)
              suffix += s" payload: message value $offset"
            assertTrue(s"Message record missing key or value: $line", line.endsWith(suffix))
          }
          offset += 1
        }
      }
    }

    def verifyNoRecordsInOutput(args: Array[String]): Unit = {
      val output = runDumpLogSegments(args)
      assertFalse(s"Data should not have been printed: $output", output.matches("(?s).*offset: [0-9]* isvalid.*"))
    }

    // Verify that records are printed with --print-data-log even if --deep-iteration is not specified
    verifyRecordsInOutput(true, Array("--print-data-log", "--files", logFilePath))
    // Verify that records are printed with --print-data-log if --deep-iteration is also specified
    verifyRecordsInOutput(true, Array("--print-data-log", "--deep-iteration", "--files", logFilePath))
    // Verify that records are printed with --value-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(true, Array("--value-decoder-class", "kafka.serializer.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --key-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(true, Array("--key-decoder-class", "kafka.serializer.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --deep-iteration even if --print-data-log is not specified
    verifyRecordsInOutput(false, Array("--deep-iteration", "--files", logFilePath))

    // Verify that records are not printed by default
    verifyNoRecordsInOutput(Array("--files", logFilePath))
  }

  @Test
  def testDumpIndexMismatches(): Unit = {
    val offsetMismatches = mutable.Map[String, List[(Long, Long)]]()
    DumpLogSegments.dumpIndex(new File(indexFilePath), indexSanityOnly = false, verifyOnly = true, offsetMismatches,
      Int.MaxValue)
    assertEquals(Map.empty, offsetMismatches)
  }

  @Test
  def testDumpTimeIndexErrors(): Unit = {
    val errors = new TimeIndexDumpErrors
    DumpLogSegments.dumpTimeIndex(new File(timeIndexFilePath), indexSanityOnly = false, verifyOnly = true, errors,
      Int.MaxValue)
    assertEquals(Map.empty, errors.misMatchesForTimeIndexFilesMap)
    assertEquals(Map.empty, errors.outOfOrderTimestamp)
    assertEquals(Map.empty, errors.shallowOffsetNotFound)
  }

  private def runDumpLogSegments(args: Array[String]): String = {
    val outContent = new ByteArrayOutputStream
    Console.withOut(outContent) {
      DumpLogSegments.main(args)
    }
    outContent.toString
  }
}
