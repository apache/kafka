/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import kafka.utils.TestUtils
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record.{RecordVersion, SimpleRecord}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Optional
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.IBP_2_8_IV1

import scala.annotation.nowarn

class PartitionWithLegacyMessageFormatTest extends AbstractPartitionTest {

  // legacy message formats are only supported with IBP < 3.0
  override protected def interBrokerProtocolVersion: MetadataVersion = IBP_2_8_IV1

  @nowarn("cat=deprecation")
  @Test
  def testMakeLeaderDoesNotUpdateEpochCacheForOldFormats(): Unit = {
    val leaderEpoch = 8
    configRepository.setTopicConfig(topicPartition.topic(),
      TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, MetadataVersion.IBP_0_10_2_IV0.shortVersion)
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)),
      magicValue = RecordVersion.V1.value
    ), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)),
      magicValue = RecordVersion.V1.value
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))
    assertEquals(None, log.latestEpoch)

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of(leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(UNDEFINED_EPOCH_OFFSET, epochEndOffset.endOffset)
    assertEquals(UNDEFINED_EPOCH, epochEndOffset.leaderEpoch)
  }

}
