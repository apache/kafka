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

import TopicPartitionStateZNode.decode
import TopicPartitionStateZNode.encode
import kafka.api.LeaderAndIsr
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.utils.Json
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.zookeeper.data.Stat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.when
import scala.jdk.CollectionConverters._

final class TopicPartitionStateZNodeTest {

  @Test
  def testEncodeDecodeRecovering(): Unit = {
    val zkVersion = 5
    val stat = mock(classOf[Stat])
    when(stat.getVersion).thenReturn(zkVersion)

    val expected = LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 6, List(1), LeaderRecoveryState.RECOVERING, zkVersion), 10)

    assertEquals(Some(expected), decode(encode(expected), stat))
  }

  @Test
  def testEncodeDecodeRecovered(): Unit = {
    val zkVersion = 5
    val stat = mock(classOf[Stat])
    when(stat.getVersion).thenReturn(zkVersion)

    val expected = LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 6, List(1), LeaderRecoveryState.RECOVERED, zkVersion), 10)

    assertEquals(Some(expected), decode(encode(expected), stat))
  }

  @Test
  def testDecodeOldValue(): Unit = {
    val zkVersion = 5
    val stat = mock(classOf[Stat])
    when(stat.getVersion).thenReturn(zkVersion)

    val expected = LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 6, List(1), LeaderRecoveryState.RECOVERED, zkVersion), 10)

    val partitionState = Map(
      "version" -> 1,
      "leader" -> expected.leaderAndIsr.leader,
      "leader_epoch" -> expected.leaderAndIsr.leaderEpoch,
      "controller_epoch" -> expected.controllerEpoch,
      "isr" -> expected.leaderAndIsr.isr.asJava
    )

    assertEquals(Some(expected), decode(Json.encodeAsBytes(partitionState.asJava), stat))
  }
}
