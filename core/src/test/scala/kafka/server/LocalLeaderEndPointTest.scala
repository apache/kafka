/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.common.OffsetAndEpoch
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

import java.util.{Map => JMap}
import scala.collection.Map
import scala.jdk.CollectionConverters._

class LocalLeaderEndPointTest extends LocalLeaderEndPointTestBase {

  @Test
  def testFetchLatestOffset(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(3L, 0), endPoint.fetchLatestOffset(topicPartition, 0))
    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(6L, 1), endPoint.fetchLatestOffset(topicPartition, 7))
  }

  @Test
  def testFetchEarliestOffset(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, 0))

    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    replicaManager.deleteRecords(timeout = 1000L, Map(topicPartition -> 3), _ => ())
    assertEquals(new OffsetAndEpoch(3L, 1), endPoint.fetchEarliestOffset(topicPartition, 7))
  }

  @Test
  def testFetchEarliestLocalOffset(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestLocalOffset(topicPartition, 0))

    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    replicaManager.logManager.getLog(topicPartition).foreach(log => log.updateLocalLogStartOffset(3))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, 7))
    assertEquals(new OffsetAndEpoch(3L, 1), endPoint.fetchEarliestLocalOffset(topicPartition, 7))
  }

  @Test
  def testFetchEpochEndOffsets(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    var result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(0))
    ).asScala

    var expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(0)
        .setEndOffset(3L)
    )

    assertEquals(expected, result)

    // Change leader epoch and end offset, and verify the behavior again.
    bumpLeaderEpoch()
    bumpLeaderEpoch()
    assertEquals(2, replicaManager.getPartitionOrException(topicPartition).getLeaderEpoch)

    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(2)
    )).asScala

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(2)
        .setEndOffset(6L)
    )

    assertEquals(expected, result)

    // Check missing epoch: 1, we expect the API to return (leader_epoch=0, end_offset=3).
    result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(1)
    )).asScala

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(0)
        .setEndOffset(3L))

    assertEquals(expected, result)

    // Check missing epoch: 5, we expect the API to return (leader_epoch=-1, end_offset=-1)
    result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(5)
    )).asScala

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(-1)
        .setEndOffset(-1L)
    )

    assertEquals(expected, result)
  }

}
