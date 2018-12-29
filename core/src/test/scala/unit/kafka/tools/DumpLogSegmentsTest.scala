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

import java.io.ByteArrayOutputStream

import kafka.log.{ Log, LogConfig, LogManager }
import kafka.server.{ BrokerTopicStats, LogDirFailureChannel }
import kafka.utils.{ MockTime, TestUtils }
import org.apache.kafka.common.record.{ CompressionType, MemoryRecords, SimpleRecord }
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{ After, Before, Test }

class DumpLogSegmentsTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val logFile = s"$logDir/00000000000000000000.log"
  val time = new MockTime(0, 0)

  @Before
  def setUp(): Unit = {
    val log = Log(logDir, LogConfig(), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

    /* append four messages */
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0,
      new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0,
      new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)
    log.flush()
  }

  @After
  def tearDown(): Unit = {
    Utils.delete(tmpDir)
  }

  @Test
  def testPrintDataLog(): Unit = {

    def verifyRecordsInOutput(args: Array[String]): Unit = {
      val output = runDumpLogSegments(args)
      val lines = output.split("\n")
      assertTrue(s"Data not printed: $output", lines.length > 2)
      // For every three message records, verify that the first line is batch-level message record and the last two lines are message records
      (0 until 6 ).foreach { i =>
        val line = lines(lines.length - 6 + i)
        if (i % 3 == 0)
          assertTrue(s"Not a valid batch-level message record: $line", line.startsWith(s"baseOffset: ${i / 3 * 2} lastOffset: "))
        else
          assertTrue(s"Not a valid message record: $line", line.startsWith(s"${DumpLogSegments.RECORD_INDENT} offset: ${i - 1 - i / 3}"))
      }
    }

    def verifyNoRecordsInOutput(args: Array[String]): Unit = {
      val output = runDumpLogSegments(args)
      assertFalse(s"Data should not have been printed: $output", output.matches("(?s).*offset: [0-9]* isvalid.*"))
    }

    // Verify that records are printed with --print-data-log even if --deep-iteration is not specified
    verifyRecordsInOutput(Array("--print-data-log", "--files", logFile))
    // Verify that records are printed with --print-data-log if --deep-iteration is also specified
    verifyRecordsInOutput(Array("--print-data-log", "--deep-iteration", "--files", logFile))
    // Verify that records are printed with --value-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(Array("--value-decoder-class", "kafka.serializer.StringDecoder", "--files", logFile))
    // Verify that records are printed with --key-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(Array("--key-decoder-class", "kafka.serializer.StringDecoder", "--files", logFile))
    // Verify that records are printed with --deep-iteration even if --print-data-log is not specified
    verifyRecordsInOutput(Array("--deep-iteration", "--files", logFile))

    // Verify that records are not printed by default
    verifyNoRecordsInOutput(Array("--files", logFile))
  }

  private def runDumpLogSegments(args: Array[String]): String = {
    val outContent = new ByteArrayOutputStream
    Console.withOut(outContent) {
      DumpLogSegments.main(args)
    }
    outContent.toString
  }
}
