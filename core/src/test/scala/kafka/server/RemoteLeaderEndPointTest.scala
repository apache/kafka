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

import kafka.cluster.BrokerEndPoint
import kafka.log.UnifiedLog
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.epoch.util.MockBlockingSender
import kafka.utils.TestUtils
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.errors.{FencedLeaderEpochException, UnknownLeaderEpochException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.mockito.Mockito.{mock, when}

import java.util
import scala.collection.Map
import scala.jdk.CollectionConverters._

class RemoteLeaderEndPointTest {

    val topicPartition = new TopicPartition("test", 0)
    val currentLeaderEpoch = 10
    val logStartOffset = 20
    val localLogStartOffset = 100
    val logEndOffset = 300
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    var blockingSend: MockBlockingSender = _
    var endPoint: LeaderEndPoint = _
    var currentBrokerEpoch = 1L

    @BeforeEach
    def setUp(): Unit = {
        val time = new MockTime
        val logPrefix = "remote-leader-endpoint"
        val sourceBroker: BrokerEndPoint = BrokerEndPoint(0, "localhost", 9092)
        val props = TestUtils.createBrokerConfig(sourceBroker.id, TestUtils.MockZkConnect, port = sourceBroker.port)
        val fetchSessionHandler = new FetchSessionHandler(new LogContext(logPrefix), sourceBroker.id)
        val config = KafkaConfig.fromProps(props)
        blockingSend = new MockBlockingSender(offsets = new util.HashMap[TopicPartition, EpochEndOffset](),
            sourceBroker = sourceBroker, time = time)
        endPoint = new RemoteLeaderEndPoint(logPrefix, blockingSend, fetchSessionHandler,
            config, replicaManager, QuotaFactory.UnboundedQuota, () => MetadataVersion.MINIMUM_KRAFT_VERSION, () => currentBrokerEpoch)
    }

    @Test
    def testFetchLatestOffset(): Unit = {
        blockingSend.setListOffsetsDataForNextResponse(Map(topicPartition ->
          new ListOffsetsPartitionResponse().setLeaderEpoch(7).setOffset(logEndOffset)))
        assertEquals(new OffsetAndEpoch(logEndOffset, 7), endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch))
    }

    @Test
    def testFetchEarliestOffset(): Unit = {
        blockingSend.setListOffsetsDataForNextResponse(Map(topicPartition ->
          new ListOffsetsPartitionResponse().setLeaderEpoch(5).setOffset(logStartOffset)))
        assertEquals(new OffsetAndEpoch(logStartOffset, 5), endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch))
    }

    @Test
    def testFetchEarliestLocalOffset(): Unit = {
        blockingSend.setListOffsetsDataForNextResponse(Map(topicPartition ->
          new ListOffsetsPartitionResponse().setLeaderEpoch(6).setOffset(localLogStartOffset)))
        assertEquals(new OffsetAndEpoch(localLogStartOffset, 6), endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch))
    }

    @Test
    def testFetchEpochEndOffsets(): Unit = {
        val expected = Map(
            topicPartition -> new EpochEndOffset()
              .setPartition(topicPartition.partition)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(0)
              .setEndOffset(logEndOffset))
        blockingSend.setOffsetsForNextResponse(expected.asJava)
        val result = endPoint.fetchEpochEndOffsets(Map(
            topicPartition -> new OffsetForLeaderPartition()
              .setPartition(topicPartition.partition)
              .setLeaderEpoch(currentLeaderEpoch)))

        assertEquals(expected, result)
    }

    @Test
    def testThrowsFencedLeaderEpochException(): Unit = {
        blockingSend.setListOffsetsDataForNextResponse(Map(topicPartition ->
          new ListOffsetsPartitionResponse().setErrorCode(Errors.FENCED_LEADER_EPOCH.code())))
        assertThrows(classOf[FencedLeaderEpochException], () => endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch - 1))
        assertThrows(classOf[FencedLeaderEpochException], () => endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch - 1))
        assertThrows(classOf[FencedLeaderEpochException], () => endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch - 1))
    }

    @Test
    def testThrowsUnknownLeaderEpochException(): Unit = {
        blockingSend.setListOffsetsDataForNextResponse(Map(topicPartition ->
          new ListOffsetsPartitionResponse().setErrorCode(Errors.UNKNOWN_LEADER_EPOCH.code())))
        assertThrows(classOf[UnknownLeaderEpochException], () => endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch + 1))
        assertThrows(classOf[UnknownLeaderEpochException], () => endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch + 1))
        assertThrows(classOf[UnknownLeaderEpochException], () => endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch + 1))
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FETCH)
    def testBrokerEpochSupplier(version: Short): Unit = {
        val tp = new TopicPartition("topic1", 0)
        val topicId1 = Uuid.randomUuid()
        val log = mock(classOf[UnifiedLog])
        val partitionMap = Map(
            tp -> PartitionFetchState(Some(topicId1), 150, None, 0, None, state = Fetching, lastFetchedEpoch = None))
        when(replicaManager.localLogOrException(tp)).thenReturn(log)
        when(log.logStartOffset).thenReturn(1)

        val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = endPoint.buildFetch(partitionMap)
        assertTrue(partitionsWithError.isEmpty)
        assertEquals(if (version < 15) -1L else 1L, fetchRequestOpt.get.fetchRequest.build(version).replicaEpoch)

        currentBrokerEpoch = 2L
        val ResultWithPartitions(newFetchRequestOpt, newPartitionsWithError) = endPoint.buildFetch(partitionMap)
        assertTrue(newPartitionsWithError.isEmpty)
        assertEquals(if (version < 15) -1L else 2L, newFetchRequestOpt.get.fetchRequest.build(version).replicaEpoch)
    }
}