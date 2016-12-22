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

import kafka.api.FetchResponsePartitionData
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Record}
import org.junit.Test
import org.junit.Assert.assertTrue

class ReplicaVerificationToolTest {

  @Test
  def testReplicaBufferVerifyChecksum(): Unit = {
    val sb = new StringBuilder

    val expectedReplicasPerTopicAndPartition = Map(
      TopicAndPartition("a", 0) -> 3,
      TopicAndPartition("a", 1) -> 3,
      TopicAndPartition("b", 0) -> 2
    )

    val replicaBuffer = new ReplicaBuffer(expectedReplicasPerTopicAndPartition, Map.empty, 2, Map.empty, 0, 0)
    expectedReplicasPerTopicAndPartition.foreach { case (tp, numReplicas) =>
      (0 until numReplicas).foreach { replicaId =>
        val records = (0 to 5).map { index =>
          Record.create(s"key $index".getBytes, s"value $index".getBytes)
        }
        val initialOffset = 4
        val memoryRecords = MemoryRecords.withRecords(initialOffset, records: _*)
        replicaBuffer.addFetchedData(tp, replicaId, new FetchResponsePartitionData(Errors.NONE.code(), hw = 20,
          new ByteBufferMessageSet(memoryRecords.buffer)))
      }
    }

    replicaBuffer.verifyCheckSum(line => sb.append(s"$line\n"))
    val output = sb.toString.trim

    assertTrue(s"Max lag information should be in output: `$output`",
      output.endsWith(": max lag is 10 for partition [a,0] at offset 10 among 3 partitions"))
  }

}
