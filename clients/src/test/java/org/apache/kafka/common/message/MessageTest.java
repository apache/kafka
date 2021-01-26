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
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
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
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
public final class MessageTest {

    private final String memberId = "memberId";
    private final String instanceId = "instanceId";

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
        List<ListOffsetsTopic> v = Collections.singletonList(new ListOffsetsTopic()
                .setName("topic")
                .setPartitions(Collections.singletonList(new ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(123L))));
        Supplier<ListOffsetsRequestData> newRequest = () -> new ListOffsetsRequestData()
                .setTopics(v)
                .setReplicaId(0);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTripsFromVersion((short) 2, newRequest.get().setIsolationLevel(IsolationLevel.READ_COMMITTED.id()));
    }

    @Test
    public void testListOffsetsResponseVersions() throws Exception {
        ListOffsetsPartitionResponse partition = new ListOffsetsPartitionResponse()
                .setErrorCode(Errors.NONE.code())
                .setPartitionIndex(0)
                .setOldStyleOffsets(Collections.singletonList(321L));
        List<ListOffsetsTopicResponse> topics = Collections.singletonList(new ListOffsetsTopicResponse()
                .setName("topic")
                .setPartitions(Collections.singletonList(partition)));
        Supplier<ListOffsetsResponseData> response = () -> new ListOffsetsResponseData()
                .setTopics(topics);
        for (short version = 0; version <= ApiKeys.LIST_OFFSETS.latestVersion(); version++) {
            ListOffsetsResponseData responseData = response.get();
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
    public void testDescribeClusterRequestVersions() throws Exception {
        testAllMessageRoundTrips(new DescribeClusterRequestData()
            .setIncludeClusterAuthorizedOperations(true));
    }

    @Test
    public void testDescribeClusterResponseVersions() throws Exception {
        DescribeClusterResponseData data = new DescribeClusterResponseData()
            .setBrokers(new DescribeClusterBrokerCollection(
                Collections.singletonList(new DescribeClusterBroker()
                    .setBrokerId(1)
                    .setHost("localhost")
                    .setPort(9092)
                    .setRack("rack1")).iterator()))
            .setClusterId("clusterId")
            .setControllerId(1)
            .setClusterAuthorizedOperations(10);

        testAllMessageRoundTrips(data);
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
                        .setPartition(0)
                        .setLeaderEpoch(3);
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataWithCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartition(0)
                        .setLeaderEpoch(3)
                        .setCurrentLeaderEpoch(5);
        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData();
        data.topics().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
                .setTopic("foo")
                .setPartitions(singletonList(partitionDataNoCurrentEpoch)));

        testAllMessageRoundTrips(data);
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
                assertThrows(NullPointerException.class, () -> testAllMessageRoundTripsFromVersion(finalVersion, allPartitionData));
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
            .setResponses(new ProduceResponseData.TopicProduceResponseCollection(singletonList(
                new ProduceResponseData.TopicProduceResponse()
                    .setName(topicName)
                    .setPartitionResponses(singletonList(
                        new ProduceResponseData.PartitionProduceResponse()
                            .setIndex(partitionIndex)
                            .setErrorCode(errorCode)
                            .setBaseOffset(baseOffset)))).iterator())));

        Supplier<ProduceResponseData> response = () -> new ProduceResponseData()
                .setResponses(new ProduceResponseData.TopicProduceResponseCollection(singletonList(
                    new ProduceResponseData.TopicProduceResponse()
                        .setName(topicName)
                        .setPartitionResponses(singletonList(
                             new ProduceResponseData.PartitionProduceResponse()
                                 .setIndex(partitionIndex)
                                 .setErrorCode(errorCode)
                                 .setBaseOffset(baseOffset)
                                 .setLogAppendTimeMs(logAppendTimeMs)
                                 .setLogStartOffset(logStartOffset)
                                 .setRecordErrors(singletonList(
                                     new ProduceResponseData.BatchIndexAndErrorMessage()
                                         .setBatchIndex(batchIndex)
                                         .setBatchIndexErrorMessage(batchIndexErrorMessage)))
                                 .setErrorMessage(errorMessage)))).iterator()))
                .setThrottleTimeMs(throttleTimeMs);

        for (short version = 0; version <= ApiKeys.PRODUCE.latestVersion(); version++) {
            ProduceResponseData responseData = response.get();

            if (version < 8) {
                responseData.responses().iterator().next().partitionResponses().get(0).setRecordErrors(Collections.emptyList());
                responseData.responses().iterator().next().partitionResponses().get(0).setErrorMessage(null);
            }

            if (version < 5) {
                responseData.responses().iterator().next().partitionResponses().get(0).setLogStartOffset(-1);
            }

            if (version < 2) {
                responseData.responses().iterator().next().partitionResponses().get(0).setLogAppendTimeMs(-1);
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
    }

    private void testEquivalentMessageRoundTrip(short version, Message message) throws Exception {
        testByteBufferRoundTrip(version, message, message);
        testJsonRoundTrip(version, message, message);
    }

    private void testByteBufferRoundTrip(short version, Message message, Message expected) throws Exception {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        assertEquals(size, buf.position(), "The result of the size function does not match the number of bytes " +
            "written for version " + version);
        Message message2 = message.getClass().getConstructor().newInstance();
        buf.flip();
        message2.read(byteBufferAccessor, version);
        assertEquals(size, buf.position(), "The result of the size function does not match the number of bytes " +
            "read back in for version " + version);
        assertEquals(expected, message2, "The message object created after a round trip did not match for " +
            "version " + version);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

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
            assertTrue(apiKey.latestVersion() <= message.highestSupportedVersion(),
                "Request message spec for " + apiKey + " only " + "supports versions up to " +
                message.highestSupportedVersion());
            try {
                message = ApiMessageType.fromApiKey(apiKey.id).newResponse();
            } catch (UnsupportedVersionException e) {
                fail("No response message spec found for API " + apiKey);
            }
            assertTrue(apiKey.latestVersion() <= message.highestSupportedVersion(),
                "Response message spec for " + apiKey + " only " + "supports versions up to " +
                message.highestSupportedVersion());
        }
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
        assertTrue(e.getMessage().contains(problemText), "Expected to get an error message about " + problemText +
            ", but got: " + e.getMessage());
    }

    private void verifyWriteSucceeds(short version, Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size * 2);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        assertEquals(size, buf.position(), "Expected the serialized size to be " + size + ", but it was " + buf.position());
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
