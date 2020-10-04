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

package kafka.tools

import kafka.utils.Exit
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchResponse
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertTrue, fail}

class ReplicaVerificationToolTest {

  @Before
  def setUp(): Unit =
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

  @After
  def tearDown(): Unit =
    Exit.resetExitProcedure()

  @Test
  def testReplicaBufferVerifyChecksum(): Unit = {
    val sb = new StringBuilder

    val expectedReplicasPerTopicAndPartition = Map(
      new TopicPartition("a", 0) -> 3,
      new TopicPartition("a", 1) -> 3,
      new TopicPartition("b", 0) -> 2
    )

    val replicaBuffer = new ReplicaBuffer(expectedReplicasPerTopicAndPartition, Map.empty, 2, 0)
    expectedReplicasPerTopicAndPartition.foreach { case (tp, numReplicas) =>
      (0 until numReplicas).foreach { replicaId =>
        val records = (0 to 5).map { index =>
          new SimpleRecord(s"key $index".getBytes, s"value $index".getBytes)
        }
        val initialOffset = 4
        val memoryRecords = MemoryRecords.withRecords(initialOffset, CompressionType.NONE, records: _*)
        val partitionData = new FetchResponse.PartitionData(Errors.NONE, 20, 20, 0L, null, memoryRecords)

        replicaBuffer.addFetchedData(tp, replicaId, partitionData)
      }
    }

    replicaBuffer.verifyCheckSum(line => sb.append(s"$line\n"))
    val output = sb.toString.trim

    // If you change this assertion, you should verify that the replica_verification_test.py system test still passes
    assertTrue(s"Max lag information should be in output: `$output`",
      output.endsWith(": max lag is 10 for partition a-1 at offset 10 among 3 partitions"))
  }

  @Test
  def testCorrectArgs(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:9091",
      "--max-wait-ms", "100",
      "--time", "-2", // earliest
      "--report-interval-ms", "10",
      "--fetch-size", "1000",
      "--topic-white-list", "topic-.+"
    )
    val opts = ReplicaVerificationTool.validateAndParseArgs(args)

    assertEquals(-2L, opts.options.valueOf(opts.initialOffsetTimeOpt))
    assertEquals(10L, opts.options.valueOf(opts.reportIntervalOpt))
    assertEquals(1000, opts.options.valueOf(opts.fetchSizeOpt))
    assertEquals(100, opts.options.valueOf(opts.maxWaitMsOpt))
    assertEquals("localhost:9091", opts.bootstrapServers)
  }

  @Test
  def testBootstrapServerArgOverridesBrokerList(): Unit = {
    val args = Array(
      "--broker-list", "localhost:9091",
      "--bootstrap-server", "localhost:9092",
    )
    val opts = ReplicaVerificationTool.validateAndParseArgs(args)
    assertEquals("localhost:9092", opts.bootstrapServers)
  }

  @Test
  def testBootstrapServerArg(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:9092"
    )
    val opts = ReplicaVerificationTool.validateAndParseArgs(args)
    assertEquals("localhost:9092", opts.bootstrapServers)
  }

  @Test
  def testBrokerListArg(): Unit = {
    val args = Array(
      "--broker-list", "localhost:9091"
    )
    val opts = ReplicaVerificationTool.validateAndParseArgs(args)
    assertEquals("localhost:9091", opts.bootstrapServers)
  }

  @Test
  def testInvalidTopicWhiteListRegexArg(): Unit = {
    val args = Array(
      "--broker-list", "localhost:9091",
      "--topic-white-list", "topic-.+\\"
    )
    shouldFailWith("is not a valid regex", args)
  }

  @Test
  def testInvalidBootstrapServerAddressArg(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:"
    )
    shouldFailWith("Please provide valid host:port", args)
  }

  def shouldFailWith(msg: String, args: Array[String]): Unit = {
    try {
      ReplicaVerificationTool.validateAndParseArgs(args)
      fail(s"Should have failed with [$msg] but no failure occurred.")
    } catch {
      case e: Exception =>
        assertTrue(s"Expected exception with message that contains:\n[$msg]\nbut it did not.\n[${e.getMessage}]",
          e.getMessage.contains(msg))
    }
  }
}
