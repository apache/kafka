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
package kafka.zk

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class ReassignPartitionsZNodeTest {

  private val topic = "foo"
  private val partition1 = 0
  private val replica1 = 1
  private val replica2 = 2

  private val reassignPartitionData = Map(new TopicPartition(topic, partition1) -> Seq(replica1, replica2))
  private val reassignmentJson = """{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]}]}"""

  @Test
  def testEncode(): Unit = {
    val encodedJsonString = new String(ReassignPartitionsZNode.encode(reassignPartitionData), StandardCharsets.UTF_8)
    assertEquals(reassignmentJson, encodedJsonString)
  }

  @Test
  def testDecodeInvalidJson(): Unit = {
    val result = ReassignPartitionsZNode.decode("invalid json".getBytes)
    val exception = result.left.getOrElse(throw new AssertionError(s"decode should have failed, result $result"))
    assertTrue(exception.isInstanceOf[JsonProcessingException])
  }

  @Test
  def testDecodeValidJson(): Unit = {
    val result = ReassignPartitionsZNode.decode(reassignmentJson.getBytes)
    val replicas = result.map(assignmentMap => assignmentMap(new TopicPartition(topic, partition1)))
    assertEquals(Right(Seq(replica1, replica2)), replicas)
  }
}
