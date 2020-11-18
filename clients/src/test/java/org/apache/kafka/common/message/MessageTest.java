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

package org.apache.kafka.common.message;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListOffsetRequestData.ListOffsetPartition;
import org.apache.kafka.common.message.ListOffsetRequestData.ListOffsetTopic;
import org.apache.kafka.common.message.ListOffsetResponseData.ListOffsetPartitionResponse;
import org.apache.kafka.common.message.ListOffsetResponseData.ListOffsetTopicResponse;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.Utils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class MessageTest {

    private final String memberId = "memberId";
    private final String instanceId = "instanceId";

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testAddOffsetsToTxnVersions() throws Exception {
        testAllMessageRoundTrips(new AddOffsetsToTxnRequestData().
                setTransactionalId("foobar").
                setProducerId(0xbadcafebadcafeL).
                setProducerEpoch((short) 123).
                setGroupId("baaz"));
        testAllMessageRoundTrips(new AddOffsetsToTxnResponseData().
                setThrottleTimeMs(42).
                setErrorCode((short) 0));
    }

    @Test
    public void testAddPartitionsToTxnVersions() throws Exception {
        testAllMessageRoundTrips(new AddPartitionsToTxnRequestData().
                setTransactionalId("blah").
                setProducerId(0xbadcafebadcafeL).
                setProducerEpoch((short) 30000).
                setTopics(new AddPartitionsToTxnTopicCollection(singletonList(
                        new AddPartitionsToTxnTopic().
                                setName("Topic").
                                setPartitions(singletonList(1))).iterator())));
    }

    @Test
    public void testCreateTopicsVersions() throws Exception {
        testAllMessageRoundTrips(new CreateTopicsRequestData().
                setTimeoutMs(1000).setTopics(new CreateTopicsRequestData.CreatableTopicCollection()));
    }

    @Test
    public void testDescribeAclsRequest() throws Exception {
        testAllMessageRoundTrips(new DescribeAclsRequestData().
                setResourceTypeFilter((byte) 42).
                setResourceNameFilter(null).
                setPatternTypeFilter((byte) 3).
                setPrincipalFilter("abc").
                setHostFilter(null).
                setOperation((byte) 0).
                setPermissionType((byte) 0));
    }

    @Test
    public void testMetadataVersions() throws Exception {
        testAllMessageRoundTrips(new MetadataRequestData().setTopics(
                Arrays.asList(new MetadataRequestData.MetadataRequestTopic().setName("foo"),
                        new MetadataRequestData.MetadataRequestTopic().setName("bar")
                )));
        testAllMessageRoundTripsFromVersion((short) 1, new MetadataRequestData().
                setTopics(null).
                setAllowAutoTopicCreation(true).
                setIncludeClusterAuthorizedOperations(false).
                setIncludeTopicAuthorizedOperations(false));
        testAllMessageRoundTripsFromVersion((short) 4, new MetadataRequestData().
                setTopics(null).
                setAllowAutoTopicCreation(false).
                setIncludeClusterAuthorizedOperations(false).
                setIncludeTopicAuthorizedOperations(false));
    }

    @Test
    public void testHeartbeatVersions() throws Exception {
        Supplier<HeartbeatRequestData> newRequest = () -> new HeartbeatRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setGenerationId(15);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTrips(newRequest.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 3, newRequest.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testJoinGroupRequestVersions() throws Exception {
        Supplier<JoinGroupRequestData> newRequest = () -> new JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setProtocolType("consumer")
                .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection())
                .setSessionTimeoutMs(10000);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTripsFromVersion((short) 1, newRequest.get().setRebalanceTimeoutMs(20000));
        testAllMessageRoundTrips(newRequest.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 5, newRequest.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testListOffsetsRequestVersions() throws Exception {
        List<ListOffsetTopic> v = Collections.singletonList(new ListOffsetTopic()
                .setName("topic")
                .setPartitions(Collections.singletonList(new ListOffsetPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(123L))));
        Supplier<ListOffsetRequestData> newRequest = () -> new ListOffsetRequestData()
                .setTopics(v)
                .setReplicaId(0);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTripsFromVersion((short) 2, newRequest.get().setIsolationLevel(IsolationLevel.READ_COMMITTED.id()));
    }

    @Test
    public void testListOffsetsResponseVersions() throws Exception {
        ListOffsetPartitionResponse partition = new ListOffsetPartitionResponse()
                .setErrorCode(Errors.NONE.code())
                .setPartitionIndex(0)
                .setOldStyleOffsets(Collections.singletonList(321L));
        List<ListOffsetTopicResponse> topics = Collections.singletonList(new ListOffsetTopicResponse()
                .setName("topic")
                .setPartitions(Collections.singletonList(partition)));
        Supplier<ListOffsetResponseData> response = () -> new ListOffsetResponseData()
                .setTopics(topics);
        for (short version = 0; version <= ApiKeys.LIST_OFFSETS.latestVersion(); version++) {
            ListOffsetResponseData responseData = response.get();
            if (version > 0) {
                responseData.topics().get(0).partitions().get(0)
                    .setOldStyleOffsets(Collections.emptyList())
                    .setOffset(456L)
                    .setTimestamp(123L);
            }
            if (version > 1) {
                responseData.setThrottleTimeMs(1000);
            }
            if (version > 3) {
                partition.setLeaderEpoch(1);
            }
            testEquivalentMessageRoundTrip(version, responseData);
        }
    }

    @Test
    public void testJoinGroupResponseVersions() throws Exception {
        Supplier<JoinGroupResponseData> newResponse = () -> new JoinGroupResponseData()
                .setMemberId(memberId)
                .setLeader(memberId)
                .setGenerationId(1)
                .setMembers(Collections.singletonList(
                        new JoinGroupResponseMember()
                                .setMemberId(memberId)
                ));
        testAllMessageRoundTrips(newResponse.get());
        testAllMessageRoundTripsFromVersion((short) 2, newResponse.get().setThrottleTimeMs(1000));
        testAllMessageRoundTrips(newResponse.get().members().get(0).setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 5, newResponse.get().members().get(0).setGroupInstanceId("instanceId"));
    }

    @Test
    public void testLeaveGroupResponseVersions() throws Exception {
        Supplier<LeaveGroupResponseData> newResponse = () -> new LeaveGroupResponseData()
                                                                 .setErrorCode(Errors.NOT_COORDINATOR.code());

        testAllMessageRoundTrips(newResponse.get());
        testAllMessageRoundTripsFromVersion((short) 1, newResponse.get().setThrottleTimeMs(1000));

        testAllMessageRoundTripsFromVersion((short) 3, newResponse.get().setMembers(
            Collections.singletonList(new MemberResponse()
            .setMemberId(memberId)
            .setGroupInstanceId(instanceId))
        ));
    }

    @Test
    public void testSyncGroupDefaultGroupInstanceId() throws Exception {
        Supplier<SyncGroupRequestData> request = () -> new SyncGroupRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setGenerationId(15)
                .setAssignments(new ArrayList<>());
        testAllMessageRoundTrips(request.get());
        testAllMessageRoundTrips(request.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 3, request.get().setGroupInstanceId(instanceId));
    }

    @Test
    public void testOffsetCommitDefaultGroupInstanceId() throws Exception {
        testAllMessageRoundTrips(new OffsetCommitRequestData()
                .setTopics(new ArrayList<>())
                .setGroupId("groupId"));

        Supplier<OffsetCommitRequestData> request = () -> new OffsetCommitRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setTopics(new ArrayList<>())
                .setGenerationId(15);
        testAllMessageRoundTripsFromVersion((short) 1, request.get());
        testAllMessageRoundTripsFromVersion((short) 1, request.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 7, request.get().setGroupInstanceId(instanceId));
    }

    @Test
    public void testDescribeGroupsRequestVersions() throws Exception {
        testAllMessageRoundTrips(new DescribeGroupsRequestData()
                .setGroups(Collections.singletonList("group"))
                .setIncludeAuthorizedOperations(false));
    }

    @Test
    public void testDescribeGroupsResponseVersions() throws Exception {
        DescribedGroupMember baseMember = new DescribedGroupMember()
            .setMemberId(memberId);

        DescribedGroup baseGroup = new DescribedGroup()
                                       .setGroupId("group")
                                       .setGroupState("Stable").setErrorCode(Errors.NONE.code())
                                       .setMembers(Collections.singletonList(baseMember))
                                       .setProtocolType("consumer");
        DescribeGroupsResponseData baseResponse = new DescribeGroupsResponseData()
                                                      .setGroups(Collections.singletonList(baseGroup));
        testAllMessageRoundTrips(baseResponse);

        testAllMessageRoundTripsFromVersion((short) 1, baseResponse.setThrottleTimeMs(10));

        baseGroup.setAuthorizedOperations(1);
        testAllMessageRoundTripsFromVersion((short) 3, baseResponse);

        baseMember.setGroupInstanceId(instanceId);
        testAllMessageRoundTripsFromVersion((short) 4, baseResponse);
    }

    @Test
    public void testGroupInstanceIdIgnorableInDescribeGroupsResponse() throws Exception {
        DescribeGroupsResponseData responseWithGroupInstanceId =
            new DescribeGroupsResponseData()
                .setGroups(Collections.singletonList(
                    new DescribedGroup()
                        .setGroupId("group")
                        .setGroupState("Stable")
                        .setErrorCode(Errors.NONE.code())
                        .setMembers(Collections.singletonList(
                            new DescribedGroupMember()
                                .setMemberId(memberId)
                                .setGroupInstanceId(instanceId)))
                        .setProtocolType("consumer")
                ));

        DescribeGroupsResponseData expectedResponse = responseWithGroupInstanceId.duplicate();
        // Unset GroupInstanceId
        expectedResponse.groups().get(0).members().get(0).setGroupInstanceId(null);

        testAllMessageRoundTripsBeforeVersion((short) 4, responseWithGroupInstanceId, expectedResponse);
    }

    @Test
    public void testThrottleTimeIgnorableInDescribeGroupsResponse() throws Exception {
        DescribeGroupsResponseData responseWithGroupInstanceId =
            new DescribeGroupsResponseData()
                .setGroups(Collections.singletonList(
                    new DescribedGroup()
                        .setGroupId("group")
                        .setGroupState("Stable")
                        .setErrorCode(Errors.NONE.code())
                        .setMembers(Collections.singletonList(
                            new DescribedGroupMember()
                                .setMemberId(memberId)))
                        .setProtocolType("consumer")
                ))
                .setThrottleTimeMs(10);

        DescribeGroupsResponseData expectedResponse = responseWithGroupInstanceId.duplicate();
        // Unset throttle time
        expectedResponse.setThrottleTimeMs(0);

        testAllMessageRoundTripsBeforeVersion((short) 1, responseWithGroupInstanceId, expectedResponse);
    }

    @Test
    public void testOffsetForLeaderEpochVersions() throws Exception {
        // Version 2 adds optional current leader epoch
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataNoCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartitionIndex(0)
                        .setLeaderEpoch(3);
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataWithCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartitionIndex(0)
                        .setLeaderEpoch(3)
                        .setCurrentLeaderEpoch(5);

        testAllMessageRoundTrips(new OffsetForLeaderEpochRequestData().setTopics(singletonList(
                new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
                        .setName("foo")
                        .setPartitions(singletonList(partitionDataNoCurrentEpoch)))
        ));
        testAllMessageRoundTripsBeforeVersion((short) 2, partitionDataWithCurrentEpoch, partitionDataNoCurrentEpoch);
        testAllMessageRoundTripsFromVersion((short) 2, partitionDataWithCurrentEpoch);

        // Version 3 adds the optional replica Id field
        testAllMessageRoundTripsFromVersion((short) 3, new OffsetForLeaderEpochRequestData().setReplicaId(5));
        testAllMessageRoundTripsBeforeVersion((short) 3,
                new OffsetForLeaderEpochRequestData().setReplicaId(5),
                new OffsetForLeaderEpochRequestData());
        testAllMessageRoundTripsBeforeVersion((short) 3,
                new OffsetForLeaderEpochRequestData().setReplicaId(5),
                new OffsetForLeaderEpochRequestData().setReplicaId(-2));
    }

    @Test
    public void testLeaderAndIsrVersions() throws Exception {
        // Version 3 adds two new fields - AddingReplicas and RemovingReplicas
        LeaderAndIsrRequestData.LeaderAndIsrTopicState partitionStateNoAddingRemovingReplicas =
            new LeaderAndIsrRequestData.LeaderAndIsrTopicState()
                .setTopicName("topic")
                .setPartitionStates(Collections.singletonList(
                    new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
                        .setPartitionIndex(0)
                        .setReplicas(Collections.singletonList(0))
                ));
        LeaderAndIsrRequestData.LeaderAndIsrTopicState partitionStateWithAddingRemovingReplicas =
            new LeaderAndIsrRequestData.LeaderAndIsrTopicState()
                .setTopicName("topic")
                .setPartitionStates(Collections.singletonList(
                    new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
                        .setPartitionIndex(0)
                        .setReplicas(Collections.singletonList(0))
                        .setAddingReplicas(Collections.singletonList(1))
                        .setRemovingReplicas(Collections.singletonList(1))
                ));
        testAllMessageRoundTripsBetweenVersions(
            (short) 2,
            (short) 3,
            new LeaderAndIsrRequestData().setTopicStates(Collections.singletonList(partitionStateWithAddingRemovingReplicas)),
            new LeaderAndIsrRequestData().setTopicStates(Collections.singletonList(partitionStateNoAddingRemovingReplicas)));
        testAllMessageRoundTripsFromVersion((short) 3, new LeaderAndIsrRequestData().setTopicStates(Collections.singletonList(partitionStateWithAddingRemovingReplicas)));
    }

    @Test
    public void testOffsetCommitRequestVersions() throws Exception {
        String groupId = "groupId";
        String topicName = "topic";
        String metadata = "metadata";
        int partition = 2;
        int offset = 100;

        testAllMessageRoundTrips(new OffsetCommitRequestData()
                                     .setGroupId(groupId)
                                     .setTopics(Collections.singletonList(
                                         new OffsetCommitRequestTopic()
                                             .setName(topicName)
                                             .setPartitions(Collections.singletonList(
                                                 new OffsetCommitRequestPartition()
                                                     .setPartitionIndex(partition)
                                                     .setCommittedMetadata(metadata)
                                                     .setCommittedOffset(offset)
                                             )))));

        Supplier<OffsetCommitRequestData> request =
            () -> new OffsetCommitRequestData()
                      .setGroupId(groupId)
                      .setMemberId("memberId")
                      .setGroupInstanceId("instanceId")
                      .setTopics(Collections.singletonList(
                          new OffsetCommitRequestTopic()
                              .setName(topicName)
                              .setPartitions(Collections.singletonList(
                                  new OffsetCommitRequestPartition()
                                      .setPartitionIndex(partition)
                                      .setCommittedLeaderEpoch(10)
                                      .setCommittedMetadata(metadata)
                                      .setCommittedOffset(offset)
                                      .setCommitTimestamp(20)
                            ))))
                    .setRetentionTimeMs(20);

        for (short version = 0; version <= ApiKeys.OFFSET_COMMIT.latestVersion(); version++) {
            OffsetCommitRequestData requestData = request.get();
            if (version < 1) {
                requestData.setMemberId("");
                requestData.setGenerationId(-1);
            }

            if (version != 1) {
                requestData.topics().get(0).partitions().get(0).setCommitTimestamp(-1);
            }

            if (version < 2 || version > 4) {
                requestData.setRetentionTimeMs(-1);
            }

            if (version < 6) {
                requestData.topics().get(0).partitions().get(0).setCommittedLeaderEpoch(-1);
            }

            if (version < 7) {
                requestData.setGroupInstanceId(null);
            }

            if (version == 1) {
                testEquivalentMessageRoundTrip(version, requestData);
            } else if (version >= 2 && version <= 4) {
                testAllMessageRoundTripsBetweenVersions(version, (short) 4, requestData, requestData);
            } else {
                testAllMessageRoundTripsFromVersion(version, requestData);
            }
        }
    }

    @Test
    public void testOffsetCommitResponseVersions() throws Exception {
        Supplier<OffsetCommitResponseData> response =
            () -> new OffsetCommitResponseData()
                      .setTopics(
                          singletonList(
                              new OffsetCommitResponseTopic()
                                  .setName("topic")
                                  .setPartitions(singletonList(
                                      new OffsetCommitResponsePartition()
                                          .setPartitionIndex(1)
                                          .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                                  ))
                          )
                      )
                      .setThrottleTimeMs(20);

        for (short version = 0; version <= ApiKeys.OFFSET_COMMIT.latestVersion(); version++) {
            OffsetCommitResponseData responseData = response.get();
            if (version < 3) {
                responseData.setThrottleTimeMs(0);
            }
            testAllMessageRoundTripsFromVersion(version, responseData);
        }
    }

    @Test
    public void testTxnOffsetCommitRequestVersions() throws Exception {
        String groupId = "groupId";
        String topicName = "topic";
        String metadata = "metadata";
        String txnId = "transactionalId";
        int producerId = 25;
        short producerEpoch = 10;
        String instanceId = "instance";
        String memberId = "member";
        int generationId = 1;

        int partition = 2;
        int offset = 100;

        testAllMessageRoundTrips(new TxnOffsetCommitRequestData()
                                     .setGroupId(groupId)
                                     .setTransactionalId(txnId)
                                     .setProducerId(producerId)
                                     .setProducerEpoch(producerEpoch)
                                     .setTopics(Collections.singletonList(
                                         new TxnOffsetCommitRequestTopic()
                                             .setName(topicName)
                                             .setPartitions(Collections.singletonList(
                                                 new TxnOffsetCommitRequestPartition()
                                                     .setPartitionIndex(partition)
                                                     .setCommittedMetadata(metadata)
                                                     .setCommittedOffset(offset)
                                             )))));

        Supplier<TxnOffsetCommitRequestData> request =
            () -> new TxnOffsetCommitRequestData()
                      .setGroupId(groupId)
                      .setTransactionalId(txnId)
                      .setProducerId(producerId)
                      .setProducerEpoch(producerEpoch)
                      .setGroupInstanceId(instanceId)
                      .setMemberId(memberId)
                      .setGenerationId(generationId)
                      .setTopics(Collections.singletonList(
                          new TxnOffsetCommitRequestTopic()
                              .setName(topicName)
                              .setPartitions(Collections.singletonList(
                                  new TxnOffsetCommitRequestPartition()
                                      .setPartitionIndex(partition)
                                      .setCommittedLeaderEpoch(10)
                                      .setCommittedMetadata(metadata)
                                      .setCommittedOffset(offset)
                              ))));

        for (short version = 0; version <= ApiKeys.TXN_OFFSET_COMMIT.latestVersion(); version++) {
            TxnOffsetCommitRequestData requestData = request.get();
            if (version < 2) {
                requestData.topics().get(0).partitions().get(0).setCommittedLeaderEpoch(-1);
            }

            if (version < 3) {
                final short finalVersion = version;
                assertThrows(UnsupportedVersionException.class, () -> testEquivalentMessageRoundTrip(finalVersion, requestData));
                requestData.setGroupInstanceId(null);
                assertThrows(UnsupportedVersionException.class, () -> testEquivalentMessageRoundTrip(finalVersion, requestData));
                requestData.setMemberId("");
                assertThrows(UnsupportedVersionException.class, () -> testEquivalentMessageRoundTrip(finalVersion, requestData));
                requestData.setGenerationId(-1);
            }

            testAllMessageRoundTripsFromVersion(version, requestData);
        }
    }

    @Test
    public void testTxnOffsetCommitResponseVersions() throws Exception {
        testAllMessageRoundTrips(
            new TxnOffsetCommitResponseData()
                .setTopics(
                   singletonList(
                       new TxnOffsetCommitResponseTopic()
                           .setName("topic")
                           .setPartitions(singletonList(
                               new TxnOffsetCommitResponsePartition()
                                   .setPartitionIndex(1)
                                   .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                           ))
                   )
               )
               .setThrottleTimeMs(20));
    }

    @Test
    public void testOffsetFetchVersions() throws Exception {
        String groupId = "groupId";
        String topicName = "topic";

        List<OffsetFetchRequestTopic> topics = Collections.singletonList(
            new OffsetFetchRequestTopic()
                .setName(topicName)
                .setPartitionIndexes(Collections.singletonList(5)));
        testAllMessageRoundTrips(new OffsetFetchRequestData()
                                     .setTopics(new ArrayList<>())
                                     .setGroupId(groupId));

        testAllMessageRoundTrips(new OffsetFetchRequestData()
                                     .setGroupId(groupId)
                                     .setTopics(topics));

        OffsetFetchRequestData allPartitionData = new OffsetFetchRequestData()
                                                      .setGroupId(groupId)
                                                      .setTopics(null);

        OffsetFetchRequestData requireStableData = new OffsetFetchRequestData()
                                                       .setGroupId(groupId)
                                                       .setTopics(topics)
                                                       .setRequireStable(true);

        for (short version = 0; version <= ApiKeys.OFFSET_FETCH.latestVersion(); version++) {
            final short finalVersion = version;
            if (version < 2) {
                assertThrows(SchemaException.class, () -> testAllMessageRoundTripsFromVersion(finalVersion, allPartitionData));
            } else {
                testAllMessageRoundTripsFromVersion(version, allPartitionData);
            }

            if (version < 7) {
                assertThrows(UnsupportedVersionException.class, () -> testAllMessageRoundTripsFromVersion(finalVersion, requireStableData));
            } else {
                testAllMessageRoundTripsFromVersion(finalVersion, requireStableData);
            }
        }

        Supplier<OffsetFetchResponseData> response =
            () -> new OffsetFetchResponseData()
                      .setTopics(Collections.singletonList(
                          new OffsetFetchResponseTopic()
                              .setName(topicName)
                              .setPartitions(Collections.singletonList(
                                  new OffsetFetchResponsePartition()
                                      .setPartitionIndex(5)
                                      .setMetadata(null)
                                      .setCommittedOffset(100)
                                      .setCommittedLeaderEpoch(3)
                                      .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())))))
                      .setErrorCode(Errors.NOT_COORDINATOR.code())
                      .setThrottleTimeMs(10);
        for (short version = 0; version <= ApiKeys.OFFSET_FETCH.latestVersion(); version++) {
            OffsetFetchResponseData responseData = response.get();
            if (version <= 1) {
                responseData.setErrorCode(Errors.NONE.code());
            }

            if (version <= 2) {
                responseData.setThrottleTimeMs(0);
            }

            if (version <= 4) {
                responseData.topics().get(0).partitions().get(0).setCommittedLeaderEpoch(-1);
            }

            testAllMessageRoundTripsFromVersion(version, responseData);
        }
    }

    @Test
    public void testProduceResponseVersions() throws Exception {
        String topicName = "topic";
        int partitionIndex = 0;
        short errorCode = Errors.INVALID_TOPIC_EXCEPTION.code();
        long baseOffset = 12L;
        int throttleTimeMs = 1234;
        long logAppendTimeMs = 1234L;
        long logStartOffset = 1234L;
        int batchIndex = 0;
        String batchIndexErrorMessage = "error message";
        String errorMessage = "global error message";

        testAllMessageRoundTrips(new ProduceResponseData()
                                     .setResponses(singletonList(
                                         new ProduceResponseData.TopicProduceResponse()
                                             .setName(topicName)
                                             .setPartitions(singletonList(
                                                 new ProduceResponseData.PartitionProduceResponse()
                                                     .setPartitionIndex(partitionIndex)
                                                     .setErrorCode(errorCode)
                                                     .setBaseOffset(baseOffset))))));

        Supplier<ProduceResponseData> response =
            () -> new ProduceResponseData()
                      .setResponses(singletonList(
                            new ProduceResponseData.TopicProduceResponse()
                                .setName(topicName)
                                .setPartitions(singletonList(
                                     new ProduceResponseData.PartitionProduceResponse()
                                         .setPartitionIndex(partitionIndex)
                                         .setErrorCode(errorCode)
                                         .setBaseOffset(baseOffset)
                                         .setLogAppendTimeMs(logAppendTimeMs)
                                         .setLogStartOffset(logStartOffset)
                                         .setRecordErrors(singletonList(
                                             new ProduceResponseData.BatchIndexAndErrorMessage()
                                                 .setBatchIndex(batchIndex)
                                                 .setBatchIndexErrorMessage(batchIndexErrorMessage)))
                                         .setErrorMessage(errorMessage)))))
                      .setThrottleTimeMs(throttleTimeMs);

        for (short version = 0; version <= ApiKeys.PRODUCE.latestVersion(); version++) {
            ProduceResponseData responseData = response.get();

            if (version < 8) {
                responseData.responses().get(0).partitions().get(0).setRecordErrors(Collections.emptyList());
                responseData.responses().get(0).partitions().get(0).setErrorMessage(null);
            }

            if (version < 5) {
                responseData.responses().get(0).partitions().get(0).setLogStartOffset(-1);
            }

            if (version < 2) {
                responseData.responses().get(0).partitions().get(0).setLogAppendTimeMs(-1);
            }

            if (version < 1) {
                responseData.setThrottleTimeMs(0);
            }

            if (version >= 3 && version <= 4) {
                testAllMessageRoundTripsBetweenVersions(version, (short) 4, responseData, responseData);
            } else if (version >= 6 && version <= 7) {
                testAllMessageRoundTripsBetweenVersions(version, (short) 7, responseData, responseData);
            } else {
                testEquivalentMessageRoundTrip(version, responseData);
            }
        }
    }

    @Test
    public void testSimpleMessage() throws Exception {
        final SimpleExampleMessageData message = new SimpleExampleMessageData();
        message.setMyStruct(new SimpleExampleMessageData.MyStruct().setStructId(25).setArrayInStruct(
            Collections.singletonList(new SimpleExampleMessageData.StructArray().setArrayFieldId(20))
        ));
        message.setMyTaggedStruct(new SimpleExampleMessageData.TaggedStruct().setStructId("abc"));

        message.setProcessId(Uuid.randomUuid());
        message.setMyNullableString("notNull");
        message.setMyInt16((short) 3);
        message.setMyString("test string");
        SimpleExampleMessageData duplicate = message.duplicate();
        assertEquals(duplicate, message);
        assertEquals(message, duplicate);
        duplicate.setMyTaggedIntArray(Collections.singletonList(123));
        assertNotEquals(duplicate, message);
        assertNotEquals(message, duplicate);

        testAllMessageRoundTripsFromVersion((short) 2, message);
    }

    private void testAllMessageRoundTrips(Message message) throws Exception {
        testDuplication(message);
        testAllMessageRoundTripsFromVersion(message.lowestSupportedVersion(), message);
    }

    private void testDuplication(Message message) {
        Message duplicate = message.duplicate();
        assertEquals(duplicate, message);
        assertEquals(message, duplicate);
        assertEquals(duplicate.hashCode(), message.hashCode());
        assertEquals(message.hashCode(), duplicate.hashCode());
    }

    private void testAllMessageRoundTripsBeforeVersion(short beforeVersion, Message message, Message expected) throws Exception {
        testAllMessageRoundTripsBetweenVersions((short) 0, beforeVersion, message, expected);
    }

    /**
     * @param startVersion - the version we want to start at, inclusive
     * @param endVersion - the version we want to end at, exclusive
     */
    private void testAllMessageRoundTripsBetweenVersions(short startVersion, short endVersion, Message message, Message expected) throws Exception {
        for (short version = startVersion; version < endVersion; version++) {
            testMessageRoundTrip(version, message, expected);
        }
    }

    private void testAllMessageRoundTripsFromVersion(short fromVersion, Message message) throws Exception {
        for (short version = fromVersion; version <= message.highestSupportedVersion(); version++) {
            testEquivalentMessageRoundTrip(version, message);
        }
    }

    private void testMessageRoundTrip(short version, Message message, Message expected) throws Exception {
        testByteBufferRoundTrip(version, message, expected);
        testStructRoundTrip(version, message, expected);
    }

    private void testEquivalentMessageRoundTrip(short version, Message message) throws Exception {
        testStructRoundTrip(version, message, message);
        testByteBufferRoundTrip(version, message, message);
        testJsonRoundTrip(version, message, message);
    }

    private void testByteBufferRoundTrip(short version, Message message, Message expected) throws Exception {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        assertEquals("The result of the size function does not match the number of bytes " +
            "written for version " + version, size, buf.position());
        Message message2 = message.getClass().getConstructor().newInstance();
        buf.flip();
        message2.read(byteBufferAccessor, version);
        assertEquals("The result of the size function does not match the number of bytes " +
            "read back in for version " + version, size, buf.position());
        assertEquals("The message object created after a round trip did not match for " +
            "version " + version, expected, message2);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

    private void testStructRoundTrip(short version, Message message, Message expected) throws Exception {
        Struct struct = message.toStruct(version);
        Message message2 = message.getClass().getConstructor().newInstance();
        message2.fromStruct(struct, version);
        assertEquals(expected, message2);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

    @SuppressWarnings("unchecked")
    private void testJsonRoundTrip(short version, Message message, Message expected) throws Exception {
        String jsonConverter = jsonConverterTypeName(message.getClass().getTypeName());
        Class<?> converter = Class.forName(jsonConverter);
        Method writeMethod = converter.getMethod("write", message.getClass(), short.class);
        JsonNode jsonNode = (JsonNode) writeMethod.invoke(null, message, version);
        Method readMethod = converter.getMethod("read", JsonNode.class, short.class);
        Message message2 = (Message) readMethod.invoke(null, jsonNode, version);
        assertEquals(expected, message2);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

    private static String jsonConverterTypeName(String source) {
        int outerClassIndex = source.lastIndexOf('$');
        if (outerClassIndex == -1) {
            return  source + "JsonConverter";
        } else {
            return source.substring(0, outerClassIndex) + "JsonConverter$" +
                source.substring(outerClassIndex + 1) + "JsonConverter";
        }
    }

    /**
     * Verify that the JSON files support the same message versions as the
     * schemas accessible through the ApiKey class.
     */
    @Test
    public void testMessageVersions() {
        for (ApiKeys apiKey : ApiKeys.values()) {
            Message message = null;
            try {
                message = ApiMessageType.fromApiKey(apiKey.id).newRequest();
            } catch (UnsupportedVersionException e) {
                fail("No request message spec found for API " + apiKey);
            }
            assertTrue("Request message spec for " + apiKey + " only " +
                    "supports versions up to " + message.highestSupportedVersion(),
                apiKey.latestVersion() <= message.highestSupportedVersion());
            try {
                message = ApiMessageType.fromApiKey(apiKey.id).newResponse();
            } catch (UnsupportedVersionException e) {
                fail("No response message spec found for API " + apiKey);
            }
            assertTrue("Response message spec for " + apiKey + " only " +
                    "supports versions up to " + message.highestSupportedVersion(),
                apiKey.latestVersion() <= message.highestSupportedVersion());
        }
    }

    /**
     * Test that the JSON request files match the schemas accessible through the ApiKey class.
     */
    @Test
    public void testRequestSchemas() {
        for (ApiKeys apiKey : ApiKeys.values()) {
            Schema[] manualSchemas = apiKey.requestSchemas;
            Schema[] generatedSchemas = ApiMessageType.fromApiKey(apiKey.id).requestSchemas();
            Assert.assertEquals("Mismatching request SCHEMAS lengths " +
                "for api key " + apiKey, manualSchemas.length, generatedSchemas.length);
            for (int v = 0; v < manualSchemas.length; v++) {
                try {
                    if (generatedSchemas[v] != null) {
                        compareTypes(manualSchemas[v], generatedSchemas[v]);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to compare request schemas " +
                        "for version " + v + " of " + apiKey, e);
                }
            }
        }
    }

    /**
     * Test that the JSON response files match the schemas accessible through the ApiKey class.
     */
    @Test
    public void testResponseSchemas() {
        for (ApiKeys apiKey : ApiKeys.values()) {
            Schema[] manualSchemas = apiKey.responseSchemas;
            Schema[] generatedSchemas = ApiMessageType.fromApiKey(apiKey.id).responseSchemas();
            Assert.assertEquals("Mismatching response SCHEMAS lengths " +
                "for api key " + apiKey, manualSchemas.length, generatedSchemas.length);
            for (int v = 0; v < manualSchemas.length; v++) {
                try {
                    if (generatedSchemas[v] != null) {
                        compareTypes(manualSchemas[v], generatedSchemas[v]);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to compare response schemas " +
                        "for version " + v + " of " + apiKey, e);
                }
            }
        }
    }

    private static class NamedType {
        final String name;
        final Type type;

        NamedType(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        boolean hasSimilarType(NamedType other) {
            if (type.getClass().equals(other.type.getClass())) {
                return true;
            }
            if (type.getClass().equals(Type.RECORDS.getClass())) {
                return other.type.getClass().equals(Type.NULLABLE_BYTES.getClass());
            } else if (type.getClass().equals(Type.NULLABLE_BYTES.getClass())) {
                return other.type.getClass().equals(Type.RECORDS.getClass());
            }
            return false;
        }

        @Override
        public String toString() {
            return name + "[" + type + "]";
        }
    }

    private static void compareTypes(Schema schemaA, Schema schemaB) {
        compareTypes(new NamedType("schemaA", schemaA),
                     new NamedType("schemaB", schemaB));
    }

    private static void compareTypes(NamedType typeA, NamedType typeB) {
        List<NamedType> listA = flatten(typeA);
        List<NamedType> listB = flatten(typeB);
        if (listA.size() != listB.size()) {
            throw new RuntimeException("Can't match up structures: typeA has " +
                Utils.join(listA, ", ") + ", but typeB has " +
                Utils.join(listB, ", "));
        }
        for (int i = 0; i < listA.size(); i++) {
            NamedType entryA = listA.get(i);
            NamedType entryB = listB.get(i);
            if (!entryA.hasSimilarType(entryB)) {
                throw new RuntimeException("Type " + entryA + " in schema A " +
                    "does not match type " + entryB + " in schema B.");
            }
            if (entryA.type.isNullable() != entryB.type.isNullable()) {
                throw new RuntimeException(String.format(
                    "Type %s in Schema A is %s, but type %s in " +
                        "Schema B is %s",
                    entryA, entryA.type.isNullable() ? "nullable" : "non-nullable",
                    entryB, entryB.type.isNullable() ? "nullable" : "non-nullable"));
            }
            if (entryA.type.isArray()) {
                compareTypes(new NamedType(entryA.name, entryA.type.arrayElementType().get()),
                             new NamedType(entryB.name, entryB.type.arrayElementType().get()));
            }
        }
    }

    /**
     * We want to remove Schema nodes from the hierarchy before doing
     * our comparison.  The reason is because Schema nodes don't change what
     * is written to the wire.  Schema(STRING, Schema(INT, SHORT)) is equivalent to
     * Schema(STRING, INT, SHORT).  This function translates schema nodes into their
     * component types.
     */
    private static List<NamedType> flatten(NamedType type) {
        if (!(type.type instanceof Schema)) {
            return singletonList(type);
        }
        Schema schema = (Schema) type.type;
        ArrayList<NamedType> results = new ArrayList<>();
        for (BoundField field : schema.fields()) {
            results.addAll(flatten(new NamedType(field.def.name, field.def.type)));
        }
        return results;
    }

    @Test
    public void testDefaultValues() {
        verifyWriteRaisesUve((short) 0, "validateOnly",
            new CreateTopicsRequestData().setValidateOnly(true));
        verifyWriteSucceeds((short) 0,
            new CreateTopicsRequestData().setValidateOnly(false));
        verifyWriteSucceeds((short) 0,
            new OffsetCommitRequestData().setRetentionTimeMs(123));
        verifyWriteRaisesUve((short) 5, "forgotten",
            new FetchRequestData().setForgottenTopicsData(singletonList(
                new FetchRequestData.ForgottenTopic().setTopic("foo"))));
    }

    @Test
    public void testNonIgnorableFieldWithDefaultNull() {
        // Test non-ignorable string field `groupInstanceId` with default null
        verifyWriteRaisesUve((short) 0, "groupInstanceId", new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId)
                .setGroupInstanceId(instanceId));
        verifyWriteSucceeds((short) 0, new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId)
                .setGroupInstanceId(null));
        verifyWriteSucceeds((short) 0, new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId));
    }

    @Test
    public void testWriteNullForNonNullableFieldRaisesException() {
        CreateTopicsRequestData createTopics = new CreateTopicsRequestData().setTopics(null);
        for (short i = (short) 0; i <= createTopics.highestSupportedVersion(); i++) {
            verifyWriteRaisesNpe(i, createTopics);
        }
        MetadataRequestData metadata = new MetadataRequestData().setTopics(null);
        verifyWriteRaisesNpe((short) 0, metadata);
    }

    @Test
    public void testUnknownTaggedFields() {
        CreateTopicsRequestData createTopics = new CreateTopicsRequestData();
        verifyWriteSucceeds((short) 6, createTopics);
        RawTaggedField field1000 = new RawTaggedField(1000, new byte[] {0x1, 0x2, 0x3});
        createTopics.unknownTaggedFields().add(field1000);
        verifyWriteRaisesUve((short) 0, "Tagged fields were set", createTopics);
        verifyWriteSucceeds((short) 6, createTopics);
    }

    private void verifyWriteRaisesNpe(short version, Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        assertThrows(NullPointerException.class, () -> {
            int size = message.size(cache, version);
            ByteBuffer buf = ByteBuffer.allocate(size);
            ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
            message.write(byteBufferAccessor, cache, version);
        });
    }

    private void verifyWriteRaisesUve(short version,
                                      String problemText,
                                      Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        UnsupportedVersionException e =
            assertThrows(UnsupportedVersionException.class, () -> {
                int size = message.size(cache, version);
                ByteBuffer buf = ByteBuffer.allocate(size);
                ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
                message.write(byteBufferAccessor, cache, version);
            });
        assertTrue("Expected to get an error message about " + problemText +
                ", but got: " + e.getMessage(),
                e.getMessage().contains(problemText));
    }

    private void verifyWriteSucceeds(short version, Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size * 2);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        ByteBuffer alt = buf.duplicate();
        alt.flip();
        StringBuilder bld = new StringBuilder();
        while (alt.hasRemaining()) {
            bld.append(String.format(" %02x", alt.get()));
        }
        assertEquals("Expected the serialized size to be " + size +
            ", but it was " + buf.position(), size, buf.position());
    }

    @Test
    public void testCompareWithUnknownTaggedFields() {
        CreateTopicsRequestData createTopics = new CreateTopicsRequestData();
        createTopics.setTimeoutMs(123);
        CreateTopicsRequestData createTopics2 = new CreateTopicsRequestData();
        createTopics2.setTimeoutMs(123);
        assertEquals(createTopics, createTopics2);
        assertEquals(createTopics2, createTopics);
        // Call the accessor, which will create a new empty list.
        createTopics.unknownTaggedFields();
        // Verify that the equalities still hold after the new empty list has been created.
        assertEquals(createTopics, createTopics2);
        assertEquals(createTopics2, createTopics);
        createTopics.unknownTaggedFields().add(new RawTaggedField(0, new byte[] {0}));
        assertNotEquals(createTopics, createTopics2);
        assertNotEquals(createTopics2, createTopics);
        createTopics2.unknownTaggedFields().add(new RawTaggedField(0, new byte[] {0}));
        assertEquals(createTopics, createTopics2);
        assertEquals(createTopics2, createTopics);
    }
}
