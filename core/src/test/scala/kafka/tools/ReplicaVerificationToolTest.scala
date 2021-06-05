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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertTrue

class ReplicaVerificationToolTest {

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
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.partition)
          .setHighWatermark(20)
          .setLastStableOffset(20)
          .setLogStartOffset(0)
          .setRecords(memoryRecords)

        replicaBuffer.addFetchedData(tp, replicaId, partitionData)
      }
    }

    replicaBuffer.verifyCheckSum(line => sb.append(s"$line\n"))
    val output = sb.toString.trim

    // If you change this assertion, you should verify that the replica_verification_test.py system test still passes
    assertTrue(output.endsWith(": max lag is 10 for partition a-1 at offset 10 among 3 partitions"),
      s"Max lag information should be in output: `$output`")
  }

}
