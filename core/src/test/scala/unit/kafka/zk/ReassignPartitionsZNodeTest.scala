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
import org.junit.Assert._
import org.junit.Test

class ReassignPartitionsZNodeTest {

  private val topic = "foo"
  private val partition1 = 0
  private val replica1 = 1
  private val replica2 = 2

  private val reassignPartitionData = Map(new TopicPartition(topic, partition1) -> Seq(replica1, replica2))
  private val reassignmentJson = """{"version":1,"partitions":[{"topic":"foo","partition":0,"replicas":[1,2]}]}"""

  @Test
  def testEncode() {
    val encodedJsonString = new String(ReassignPartitionsZNode.encode(reassignPartitionData), StandardCharsets.UTF_8)
    assertEquals(reassignmentJson, encodedJsonString)
  }

  @Test
  def testDecodeInvalidJson() {
    val result = ReassignPartitionsZNode.decode("invalid json".getBytes)
    assertTrue(result.isLeft)
    assertTrue(result.left.get.isInstanceOf[JsonProcessingException])
  }

  @Test
  def testDecodeValidJson() {
    val result = ReassignPartitionsZNode.decode(reassignmentJson.getBytes)
    assertTrue(result.isRight)
    val assignmentMap = result.right.get
    assertEquals(Seq(replica1, replica2), assignmentMap(new TopicPartition(topic, partition1)))
  }
}
