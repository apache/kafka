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
package org.apache.kafka.raft;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestDataJsonConverter;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseDataJsonConverter;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestDataJsonConverter;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestDataJsonConverter;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestDataJsonConverter;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseDataJsonConverter;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteRequestDataJsonConverter;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VoteResponseDataJsonConverter;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RaftUtilTest {

    private final TopicPartition topicPartition = new TopicPartition("topic", 1);
    private final ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    private final InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 9990);
    private final String clusterId = "I4ZmrWqfT2e-upky_4fdPA";
    private static final Uuid TEST_DIRECTORY_ID1 = Uuid.randomUuid();
    private static final Uuid TEST_DIRECTORY_ID2 = Uuid.randomUuid();

    @Test
    public void testErrorResponse() {
        assertEquals(new VoteResponseData().setErrorCode(Errors.NONE.code()),
                RaftUtil.errorResponse(ApiKeys.VOTE, Errors.NONE));
        assertEquals(new BeginQuorumEpochResponseData().setErrorCode(Errors.NONE.code()),
                RaftUtil.errorResponse(ApiKeys.BEGIN_QUORUM_EPOCH, Errors.NONE));
        assertEquals(new EndQuorumEpochResponseData().setErrorCode(Errors.NONE.code()),
                RaftUtil.errorResponse(ApiKeys.END_QUORUM_EPOCH, Errors.NONE));
        assertEquals(new FetchResponseData().setErrorCode(Errors.NONE.code()),
                RaftUtil.errorResponse(ApiKeys.FETCH, Errors.NONE));
        assertEquals(new FetchSnapshotResponseData().setErrorCode(Errors.NONE.code()),
                RaftUtil.errorResponse(ApiKeys.FETCH_SNAPSHOT, Errors.NONE));
        assertThrows(IllegalArgumentException.class, () -> RaftUtil.errorResponse(ApiKeys.PRODUCE, Errors.NONE));
    }

    private static Stream<Arguments> singletonFetchRequestTestCases() {
        return Stream.of(
                Arguments.of(new FetchRequestTestCase(Uuid.ZERO_UUID, (short) 4, (short) -1,
                        "{\"replicaId\":-1,\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0," +
                            "\"topics\":[{\"topic\":\"topic\",\"partitions\":[{\"partition\":2,\"fetchOffset\":333," +
                            "\"partitionMaxBytes\":10}]}]}")),
                Arguments.of(new FetchRequestTestCase(Uuid.ZERO_UUID, (short) 5, (short) -1,
                        "{\"replicaId\":-1,\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0," +
                            "\"topics\":[{\"topic\":\"topic\",\"partitions\":[{\"partition\":2,\"fetchOffset\":333," +
                            "\"logStartOffset\":0,\"partitionMaxBytes\":10}]}]}")),
                Arguments.of(new FetchRequestTestCase(Uuid.ZERO_UUID, (short) 7, (short) -1,
                        "{\"replicaId\":-1,\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0," +
                            "\"sessionId\":0,\"sessionEpoch\":-1,\"topics\":[{\"topic\":\"topic\",\"partitions\":[{" +
                            "\"partition\":2,\"fetchOffset\":333,\"logStartOffset\":0,\"partitionMaxBytes\":10}]}]," +
                            "\"forgottenTopicsData\":[]}")),
                Arguments.of(new FetchRequestTestCase(Uuid.ZERO_UUID, (short) 11, (short) -1,
                        "{\"replicaId\":-1,\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0," +
                            "\"sessionId\":0,\"sessionEpoch\":-1,\"topics\":[{\"topic\":\"topic\",\"partitions\":[{" +
                            "\"partition\":2,\"currentLeaderEpoch\":5,\"fetchOffset\":333,\"logStartOffset\":0," +
                            "\"partitionMaxBytes\":10}]}],\"forgottenTopicsData\":[],\"rackId\":\"\"}")),
                Arguments.of(new FetchRequestTestCase(Uuid.ZERO_UUID, (short) 12, (short) 10,
                        "{\"replicaId\":-1,\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0," +
                            "\"sessionId\":0,\"sessionEpoch\":-1,\"topics\":[{\"topic\":\"topic\",\"partitions\":[{" +
                            "\"partition\":2,\"currentLeaderEpoch\":5,\"fetchOffset\":333,\"lastFetchedEpoch\":10," +
                            "\"logStartOffset\":0,\"partitionMaxBytes\":10}]}],\"forgottenTopicsData\":[],\"rackId\":\"\"}")),
                Arguments.of(new FetchRequestTestCase(Uuid.ZERO_UUID, (short) 15, (short) 10,
                        "{\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0,\"sessionId\":0," +
                            "\"sessionEpoch\":-1,\"topics\":[{\"topicId\":\"AAAAAAAAAAAAAAAAAAAAAQ\",\"partitions\":[{" +
                            "\"partition\":2,\"currentLeaderEpoch\":5,\"fetchOffset\":333,\"lastFetchedEpoch\":10," +
                            "\"logStartOffset\":0,\"partitionMaxBytes\":10}]}],\"forgottenTopicsData\":[],\"rackId\":\"\"}")),
                Arguments.of(new FetchRequestTestCase(Uuid.ONE_UUID, (short) 17, (short) 10,
                        "{\"maxWaitMs\":0,\"minBytes\":0,\"maxBytes\":2147483647,\"isolationLevel\":0,\"sessionId\":0," +
                            "\"sessionEpoch\":-1,\"topics\":[{\"topicId\":\"AAAAAAAAAAAAAAAAAAAAAQ\",\"partitions\":[{" +
                            "\"partition\":2,\"currentLeaderEpoch\":5,\"fetchOffset\":333,\"lastFetchedEpoch\":10," +
                            "\"logStartOffset\":0,\"partitionMaxBytes\":10,\"replicaDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAQ\"}]}]," +
                            "\"forgottenTopicsData\":[],\"rackId\":\"\"}"))
        );
    }

    private static Stream<Arguments> singletonFetchResponseTestCases() {
        return Stream.of(
                Arguments.of(new FetchResponseTestCase((short) 4, -1,
                        "{\"throttleTimeMs\":0,\"responses\":[{\"topic\":\"topic\",\"partitions\":" +
                            "[{\"partitionIndex\":1,\"errorCode\":0,\"highWatermark\":1000,\"lastStableOffset\":900," +
                            "\"abortedTransactions\":[{\"producerId\":1,\"firstOffset\":10}],\"records\":\"\"}]}]}")),
                Arguments.of(new FetchResponseTestCase((short) 5, -1,
                        "{\"throttleTimeMs\":0,\"responses\":[{\"topic\":\"topic\",\"partitions\":" +
                            "[{\"partitionIndex\":1,\"errorCode\":0,\"highWatermark\":1000,\"lastStableOffset\":900," +
                            "\"logStartOffset\":10,\"abortedTransactions\":[{\"producerId\":1,\"firstOffset\":10}]," +
                            "\"records\":\"\"}]}]}")),
                Arguments.of(new FetchResponseTestCase((short) 7, -1,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"sessionId\":0,\"responses\":[{" +
                            "\"topic\":\"topic\",\"partitions\":[{\"partitionIndex\":1,\"errorCode\":0," +
                            "\"highWatermark\":1000,\"lastStableOffset\":900,\"logStartOffset\":10," +
                            "\"abortedTransactions\":[{\"producerId\":1,\"firstOffset\":10}],\"records\":\"\"}]}]}")),
                Arguments.of(new FetchResponseTestCase((short) 11, 21,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"sessionId\":0,\"responses\":[{\"topic\":\"topic\"" +
                            ",\"partitions\":[{\"partitionIndex\":1,\"errorCode\":0,\"highWatermark\":1000," +
                            "\"lastStableOffset\":900,\"logStartOffset\":10,\"abortedTransactions\":[{\"producerId\":1," +
                            "\"firstOffset\":10}],\"preferredReadReplica\":21,\"records\":\"\"}]}]}")),
                Arguments.of(new FetchResponseTestCase((short) 12, 21,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"sessionId\":0,\"responses\":[{" +
                            "\"topic\":\"topic\",\"partitions\":[{\"partitionIndex\":1,\"errorCode\":0," +
                            "\"highWatermark\":1000,\"lastStableOffset\":900,\"logStartOffset\":10,\"abortedTransactions\"" +
                            ":[{\"producerId\":1,\"firstOffset\":10}],\"preferredReadReplica\":21,\"records\":\"\"}]}]}")),
                Arguments.of(new FetchResponseTestCase((short) 13, 21,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"sessionId\":0,\"responses\":[{" +
                            "\"topicId\":\"AAAAAAAAAAAAAAAAAAAAAQ\",\"partitions\":[{\"partitionIndex\":1,\"errorCode\":0," +
                            "\"highWatermark\":1000,\"lastStableOffset\":900,\"logStartOffset\":10,\"abortedTransactions\":[{" +
                            "\"producerId\":1,\"firstOffset\":10}],\"preferredReadReplica\":21,\"records\":\"\"}]}]}")),
                Arguments.of(new FetchResponseTestCase((short) 16, 21,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"sessionId\":0,\"responses\":[{" +
                            "\"topicId\":\"AAAAAAAAAAAAAAAAAAAAAQ\",\"partitions\":[{\"partitionIndex\":1," +
                            "\"errorCode\":0,\"highWatermark\":1000,\"lastStableOffset\":900,\"logStartOffset\":10," +
                            "\"abortedTransactions\":[{\"producerId\":1,\"firstOffset\":10}],\"preferredReadReplica\":21," +
                            "\"records\":\"\"}]}]}"))
        );
    }

    private static Stream<Arguments> voteRequestTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"topics\":[{\"topicName\":\"topic\"," +
                            "\"partitions\":[{\"partitionIndex\":1,\"replicaEpoch\":1,\"replicaId\":1," +
                            "\"lastOffsetEpoch\":1000,\"lastOffset\":1000}]}]}"),
                Arguments.of((short) 1,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"voterId\":2,\"topics\":[{" +
                            "\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1,\"replicaEpoch\":1," +
                            "\"replicaId\":1,\"replicaDirectoryId\":\"" + TEST_DIRECTORY_ID1 + "\"," +
                            "\"voterDirectoryId\":\"" + TEST_DIRECTORY_ID2 + "\",\"lastOffsetEpoch\":1000," +
                            "\"lastOffset\":1000}]}]}"),
                Arguments.of((short) 2,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"voterId\":2,\"topics\":[{" +
                            "\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1,\"replicaEpoch\":1," +
                            "\"replicaId\":1,\"replicaDirectoryId\":\"" + TEST_DIRECTORY_ID1 + "\"," +
                            "\"voterDirectoryId\":\"" + TEST_DIRECTORY_ID2 + "\",\"lastOffsetEpoch\":1000," +
                            "\"lastOffset\":1000,\"preVote\":true}]}]}"),
                Arguments.of((short) 2,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"voterId\":2,\"topics\":[{" +
                            "\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1,\"replicaEpoch\":1," +
                            "\"replicaId\":1,\"replicaDirectoryId\":\"" + TEST_DIRECTORY_ID1 + "\"," +
                            "\"voterDirectoryId\":\"" + TEST_DIRECTORY_ID2 + "\",\"lastOffsetEpoch\":1000," +
                            "\"lastOffset\":1000,\"preVote\":true}]}]}")
        );
    }

    private static Stream<Arguments> voteResponseTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":0,\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1,\"voteGranted\":true}]}]}"),
                Arguments.of((short) 1,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":0,\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1,\"voteGranted\":true}]}]," +
                            "\"nodeEndpoints\":[{\"nodeId\":1,\"host\":\"localhost\",\"port\":9990}]}"),
                Arguments.of((short) 2,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":0,\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1,\"voteGranted\":true}]}]," +
                            "\"nodeEndpoints\":[{\"nodeId\":1,\"host\":\"localhost\",\"port\":9990}]}")
        );
    }

    private static Stream<Arguments> fetchSnapshotRequestTestCases() {
        return Stream.of(
                Arguments.of((short) 0, ReplicaKey.NO_DIRECTORY_ID,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"replicaId\":1,\"maxBytes\":1000,\"topics\":[{" +
                            "\"name\":\"topic\",\"partitions\":[{\"partition\":1,\"currentLeaderEpoch\":1," +
                            "\"snapshotId\":{\"endOffset\":10,\"epoch\":1},\"position\":10}]}]}"),
                Arguments.of((short) 1, Uuid.ONE_UUID,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"replicaId\":1,\"maxBytes\":1000,\"topics\":[{" +
                            "\"name\":\"topic\",\"partitions\":[{\"partition\":1,\"currentLeaderEpoch\":1,\"snapshotId\":{" +
                            "\"endOffset\":10,\"epoch\":1},\"position\":10," +
                            "\"replicaDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAQ\"}]}]}")
        );
    }

    private static Stream<Arguments> fetchSnapshotResponseTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"topics\":[{\"name\":\"topic\",\"partitions\":[{" +
                            "\"index\":1,\"errorCode\":0,\"snapshotId\":{\"endOffset\":0,\"epoch\":0},\"size\":0," +
                            "\"position\":0,\"unalignedRecords\":\"\"}]}]}"),
                Arguments.of((short) 1,
                        "{\"throttleTimeMs\":0,\"errorCode\":0,\"topics\":[{\"name\":\"topic\",\"partitions\":[{\"index\":1," +
                            "\"errorCode\":0,\"snapshotId\":{\"endOffset\":0,\"epoch\":0},\"size\":0,\"position\":0," +
                            "\"unalignedRecords\":\"\"}]}],\"nodeEndpoints\":[{\"nodeId\":1,\"host\":\"localhost\"," +
                            "\"port\":9990}]}")
        );
    }

    private static Stream<Arguments> beginQuorumEpochRequestTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":1,\"leaderId\":1,\"leaderEpoch\":1}]}]}"),
                Arguments.of((short) 1,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"voterId\":1,\"topics\":[{\"topicName\":\"topic\"," +
                            "\"partitions\":[{\"partitionIndex\":1,\"voterDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAQ\"," +
                            "\"leaderId\":1,\"leaderEpoch\":1}]}],\"leaderEndpoints\":[{\"name\":\"PLAINTEXT\"," +
                            "\"host\":\"localhost\",\"port\":9990}]}")
        );
    }

    private static Stream<Arguments> beginQuorumEpochResponseTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":0,\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1}]}]}"),
                Arguments.of((short) 1,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":0,\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1}]}]," +
                            "\"nodeEndpoints\":[{\"nodeId\":1,\"host\":\"localhost\",\"port\":9990}]}")
        );
    }

    private static Stream<Arguments> endQuorumEpochRequestTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":1,\"leaderId\":1,\"leaderEpoch\":1,\"preferredSuccessors\":[1]}]}]}"),
                Arguments.of((short) 1,
                        "{\"clusterId\":\"I4ZmrWqfT2e-upky_4fdPA\",\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":1,\"leaderId\":1,\"leaderEpoch\":1,\"preferredCandidates\":[{" +
                            "\"candidateId\":1,\"candidateDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAQ\"}]}]}],\"" +
                            "leaderEndpoints\":[]}")
        );
    }

    private static Stream<Arguments> endQuorumEpochResponseTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":0," +
                            "\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1}]}]}"),
                Arguments.of((short) 1,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":0," +
                            "\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1}]}],\"nodeEndpoints\":[{\"nodeId\":1,\"host\":\"localhost\",\"port\":9990}]}")
        );
    }

    private static Stream<Arguments> describeQuorumRequestTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                    "{\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1}]}]}"),
                Arguments.of((short) 1,
                    "{\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1}]}]}"),
                Arguments.of((short) 2,
                    "{\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1}]}]}")
        );
    }

    private static Stream<Arguments> describeQuorumResponseTestCases() {
        return Stream.of(
                Arguments.of((short) 0,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1," +
                            "\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1,\"highWatermark\":1000,\"currentVoters\":[{" +
                            "\"replicaId\":1,\"logEndOffset\":-1}],\"observers\":[{\"replicaId\":1,\"logEndOffset\":-1}]}]}]}"),
                Arguments.of((short) 1,
                        "{\"errorCode\":0,\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{\"partitionIndex\":1," +
                            "\"errorCode\":0,\"leaderId\":1,\"leaderEpoch\":1,\"highWatermark\":1000,\"currentVoters\":" +
                            "[{\"replicaId\":1,\"logEndOffset\":-1,\"lastFetchTimestamp\":0,\"lastCaughtUpTimestamp\":0}]," +
                            "\"observers\":[{\"replicaId\":1,\"logEndOffset\":-1,\"lastFetchTimestamp\":0,\"lastCaughtUpTimestamp\":0}]}]}]}"),
                Arguments.of((short) 2,
                        "{\"errorCode\":0,\"errorMessage\":\"\",\"topics\":[{\"topicName\":\"topic\",\"partitions\":[{" +
                            "\"partitionIndex\":1,\"errorCode\":0,\"errorMessage\":\"\",\"leaderId\":1,\"leaderEpoch\":1," +
                            "\"highWatermark\":1000,\"currentVoters\":[{\"replicaId\":1,\"replicaDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAQ\"," +
                            "\"logEndOffset\":-1,\"lastFetchTimestamp\":0,\"lastCaughtUpTimestamp\":0}],\"observers\":[{" +
                            "\"replicaId\":1,\"replicaDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAQ\",\"logEndOffset\":-1," +
                            "\"lastFetchTimestamp\":0,\"lastCaughtUpTimestamp\":0}]}]}],\"nodes\":[{\"nodeId\":1,\"listeners\":[]}]}")
        );
    }

    @ParameterizedTest
    @MethodSource("singletonFetchRequestTestCases")
    public void testSingletonFetchRequestForAllVersion(final FetchRequestTestCase testCase) {
        FetchRequestData fetchRequestData = RaftUtil.singletonFetchRequest(topicPartition, Uuid.ONE_UUID,
                partition -> partition
                        .setPartitionMaxBytes(10)
                        .setCurrentLeaderEpoch(5)
                        .setFetchOffset(333)
                        .setLastFetchedEpoch(testCase.lastFetchedEpoch)
                        .setPartition(2)
                        .setReplicaDirectoryId(testCase.replicaDirectoryId)
                        .setLogStartOffset(0)
        );
        JsonNode json = FetchRequestDataJsonConverter.write(fetchRequestData, testCase.version);
        assertEquals(testCase.expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("singletonFetchResponseTestCases")
    public void testSingletonFetchResponseForAllVersion(final FetchResponseTestCase testCase) {
        final long highWatermark = 1000;
        final long logStartOffset = 10;
        final long lastStableOffset = 900;
        final int leaderId = 0;
        final int producerId = 1;
        final int firstOffset = 10;

        FetchResponseData fetchResponseData = RaftUtil.singletonFetchResponse(
                listenerName,
                testCase.version,
                topicPartition,
                Uuid.ONE_UUID,
                Errors.NONE,
                leaderId,
                Endpoints.empty(),
                partition -> partition
                        .setPartitionIndex(topicPartition.partition())
                        .setHighWatermark(highWatermark)
                        .setRecords(createRecords())
                        .setLogStartOffset(logStartOffset)
                        .setLastStableOffset(lastStableOffset)
                        .setPreferredReadReplica(testCase.preferredReadReplicaId)
                        .setSnapshotId(new FetchResponseData.SnapshotId())
                        .setErrorCode(Errors.NONE.code())
                        .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch())
                        .setAbortedTransactions(singletonList(
                                new FetchResponseData.AbortedTransaction()
                                        .setProducerId(producerId)
                                        .setFirstOffset(firstOffset)
                        ))
                        .setDivergingEpoch(new FetchResponseData.EpochEndOffset())
        );
        JsonNode json = FetchResponseDataJsonConverter.write(fetchResponseData, testCase.version);
        assertEquals(testCase.expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("voteRequestTestCases")
    public void testSingletonVoteRequestForAllVersion(final short version, final String expectedJson) {
        int replicaEpoch = 1;
        int lastEpoch = 1000;
        long lastEpochOffset = 1000;

        VoteRequestData voteRequestData = RaftUtil.singletonVoteRequest(
            topicPartition,
            clusterId,
            replicaEpoch,
            ReplicaKey.of(1, TEST_DIRECTORY_ID1),
            ReplicaKey.of(2, TEST_DIRECTORY_ID2),
            lastEpoch,
            lastEpochOffset,
            version >= 2
        );
        JsonNode json = VoteRequestDataJsonConverter.write(voteRequestData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("voteResponseTestCases")
    public void testSingletonVoteResponseForAllVersion(final short version, final String expectedJson) {
        int leaderEpoch = 1;
        int leaderId = 1;

        VoteResponseData voteResponseData = RaftUtil.singletonVoteResponse(
                listenerName,
                version,
                Errors.NONE,
                topicPartition,
                Errors.NONE,
                leaderEpoch,
                leaderId,
                true,
                Endpoints.fromInetSocketAddresses(singletonMap(listenerName, address))
        );
        JsonNode json = VoteResponseDataJsonConverter.write(voteResponseData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("fetchSnapshotRequestTestCases")
    public void testSingletonFetchSnapshotRequestForAllVersion(final short version,
                                                               final Uuid directoryId,
                                                               final String expectedJson) {
        int epoch = 1;
        int maxBytes = 1000;
        int position = 10;

        FetchSnapshotRequestData fetchSnapshotRequestData = RaftUtil.singletonFetchSnapshotRequest(
                clusterId,
                ReplicaKey.of(1, directoryId),
                topicPartition,
                epoch,
                new OffsetAndEpoch(10, epoch),
                maxBytes,
                position
        );
        JsonNode json = FetchSnapshotRequestDataJsonConverter.write(fetchSnapshotRequestData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("fetchSnapshotResponseTestCases")
    public void testSingletonFetchSnapshotResponseForAllVersion(final short version, final String expectedJson) {
        int leaderId = 1;

        FetchSnapshotResponseData fetchSnapshotResponseData = RaftUtil.singletonFetchSnapshotResponse(
                listenerName,
                version,
                topicPartition,
                leaderId,
                Endpoints.fromInetSocketAddresses(singletonMap(listenerName, address)),
                responsePartitionSnapshot -> responsePartitionSnapshot
        );

        JsonNode json = FetchSnapshotResponseDataJsonConverter.write(fetchSnapshotResponseData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("beginQuorumEpochRequestTestCases")
    public void testSingletonBeginQuorumEpochRequestForAllVersion(final short version, final String expectedJson) {
        int leaderEpoch = 1;
        int leaderId = 1;

        BeginQuorumEpochRequestData beginQuorumEpochRequestData = RaftUtil.singletonBeginQuorumEpochRequest(
                topicPartition,
                clusterId,
                leaderEpoch,
                leaderId,
                Endpoints.fromInetSocketAddresses(singletonMap(listenerName, address)),
                ReplicaKey.of(1, Uuid.ONE_UUID)
        );
        JsonNode json = BeginQuorumEpochRequestDataJsonConverter.write(beginQuorumEpochRequestData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("beginQuorumEpochResponseTestCases")
    public void testSingletonBeginQuorumEpochResponseForAllVersion(final short version, final String expectedJson) {
        int leaderEpoch = 1;
        int leaderId = 1;

        BeginQuorumEpochResponseData beginQuorumEpochResponseData = RaftUtil.singletonBeginQuorumEpochResponse(
                listenerName,
                version,
                Errors.NONE,
                topicPartition,
                Errors.NONE,
                leaderEpoch,
                leaderId,
                Endpoints.fromInetSocketAddresses(singletonMap(listenerName, address))
        );
        JsonNode json = BeginQuorumEpochResponseDataJsonConverter.write(beginQuorumEpochResponseData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("endQuorumEpochRequestTestCases")
    public void testSingletonEndQuorumEpochRequestForAllVersion(final short version, final String expectedJson) {
        int leaderEpoch = 1;
        int leaderId = 1;

        EndQuorumEpochRequestData endQuorumEpochRequestData = RaftUtil.singletonEndQuorumEpochRequest(
                topicPartition,
                clusterId,
                leaderEpoch,
                leaderId,
                singletonList(ReplicaKey.of(1, Uuid.ONE_UUID))
        );
        JsonNode json = EndQuorumEpochRequestDataJsonConverter.write(endQuorumEpochRequestData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("endQuorumEpochResponseTestCases")
    public void testSingletonEndQuorumEpochResponseForAllVersion(final short version, final String expectedJson) {
        int leaderEpoch = 1;
        int leaderId = 1;

        EndQuorumEpochResponseData endQuorumEpochResponseData = RaftUtil.singletonEndQuorumEpochResponse(
                listenerName,
                version,
                Errors.NONE,
                topicPartition,
                Errors.NONE,
                leaderEpoch,
                leaderId,
                Endpoints.fromInetSocketAddresses(singletonMap(listenerName, address))
        );
        JsonNode json = EndQuorumEpochResponseDataJsonConverter.write(endQuorumEpochResponseData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("describeQuorumRequestTestCases")
    public void testSingletonDescribeQuorumRequestForAllVersion(final short version, final String expectedJson) {
        DescribeQuorumRequestData describeQuorumRequestData = RaftUtil.singletonDescribeQuorumRequest(topicPartition);
        JsonNode json = DescribeQuorumRequestDataJsonConverter.write(describeQuorumRequestData, version);
        assertEquals(expectedJson, json.toString());
    }

    @ParameterizedTest
    @MethodSource("describeQuorumResponseTestCases")
    public void testSingletonDescribeQuorumResponseForAllVersion(final short version, final String expectedJson) {
        int leaderEpoch = 1;
        int leaderId = 1;
        long highWatermark = 1000;
        ReplicaKey replicaKey = ReplicaKey.of(1, Uuid.ONE_UUID);
        LeaderState.ReplicaState replicaState =
                new LeaderState.ReplicaState(replicaKey, true, Endpoints.empty());

        DescribeQuorumResponseData describeQuorumResponseData = RaftUtil.singletonDescribeQuorumResponse(
                version,
                topicPartition,
                leaderId,
                leaderEpoch,
                highWatermark,
                Collections.singletonList(replicaState),
                Collections.singletonList(replicaState),
                0
        );
        JsonNode json = DescribeQuorumResponseDataJsonConverter.write(describeQuorumResponseData, version);
        assertEquals(expectedJson, json.toString());
    }

    @Test
    public void testAddVoterResponse() {
        for (Errors value : Errors.values()) {
            AddRaftVoterResponseData addRaftVoterResponseData = RaftUtil.addVoterResponse(value, value.message());
            assertEquals(value.code(), addRaftVoterResponseData.errorCode());
            assertEquals(value.message(), addRaftVoterResponseData.errorMessage());
        }
    }

    @Test
    public void testRemoveVoterResponse() {
        for (Errors value : Errors.values()) {
            RemoveRaftVoterResponseData removeRaftVoterResponseData = RaftUtil.removeVoterResponse(value, value.message());
            assertEquals(value.code(), removeRaftVoterResponseData.errorCode());
            assertEquals(value.message(), removeRaftVoterResponseData.errorMessage());
        }
    }

    private Records createRecords() {
        ByteBuffer allocate = ByteBuffer.allocate(1024);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0)) {
            for (int i = 0; i < 10; ++i) {
                builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());
            }
            return builder.build();
        }
    }

    private static class FetchRequestTestCase {
        private final Uuid replicaDirectoryId;
        private final short version;
        private final short lastFetchedEpoch;
        private final String expectedJson;

        private FetchRequestTestCase(Uuid replicaDirectoryId, short version,
                                     short lastFetchedEpoch, String expectedJson) {
            this.replicaDirectoryId = replicaDirectoryId;
            this.version = version;
            this.lastFetchedEpoch = lastFetchedEpoch;
            this.expectedJson = expectedJson;
        }
    }

    private static class FetchResponseTestCase {
        private final short version;
        private final int preferredReadReplicaId;
        private final String expectedJson;

        private FetchResponseTestCase(short version,
                                      int preferredReadReplicaId,
                                      String expectedJson) {
            this.version = version;
            this.preferredReadReplicaId = preferredReadReplicaId;
            this.expectedJson = expectedJson;
        }
    }
}
