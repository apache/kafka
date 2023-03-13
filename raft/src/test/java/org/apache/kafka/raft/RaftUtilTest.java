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
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RaftUtilTest {

    private String topic = "__cluster_metadata";
    private Uuid topicId = Uuid.METADATA_TOPIC_ID;
    private int partition = 0;

    @Test
    public void testValidateFetchRequestData() {
        FetchRequestData none = new FetchRequestData()
            .setTopics(Collections.singletonList(
                new FetchRequestData.FetchTopic().setTopic(topic).setTopicId(topicId)
                    .setPartitions(Collections.singletonList(new FetchRequestData.FetchPartition().setPartition(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition), topicId)
        );

        List<FetchRequestData> invalidRequests = Arrays.asList(
            new FetchRequestData(),
            new FetchRequestData().setTopics(
                Arrays.asList(
                    new FetchRequestData.FetchTopic(),
                    new FetchRequestData.FetchTopic())),
            new FetchRequestData().setTopics(
                Collections.singletonList(
                    new FetchRequestData.FetchTopic()
                        .setTopic(topic)
                        .setTopicId(topicId)
                )
            ),
            new FetchRequestData().setTopics(
                Collections.singletonList(
                    new FetchRequestData.FetchTopic()
                        .setTopic(topic)
                        .setTopicId(topicId)
                        .setPartitions(
                            Arrays.asList(
                                new FetchRequestData.FetchPartition(),
                                new FetchRequestData.FetchPartition()
                            )
                        )
                )
            )
        );
        for (FetchRequestData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition), topicId)
            );
        }

        List<FetchRequestData> unknownTopicOrPartitionRequests = Arrays.asList(
            new FetchRequestData().setTopics(
                Collections.singletonList(
                    new FetchRequestData.FetchTopic()
                        .setTopic(topic)
                        .setTopicId(Uuid.randomUuid())
                        .setPartitions(
                            Collections.singletonList(new FetchRequestData.FetchPartition().setPartition(0))
                        )
                )
            ),
            new FetchRequestData().setTopics(
                Collections.singletonList(
                    new FetchRequestData.FetchTopic()
                        .setTopic(topic)
                        .setTopicId(topicId)
                        .setPartitions(
                            Collections.singletonList(new FetchRequestData.FetchPartition().setPartition(1))
                        )
                )
            )
        );
        for (FetchRequestData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition), topicId)
            );
        }
    }

    @Test
    public void testValidateFetchResponseData() {
        FetchResponseData none = new FetchResponseData()
            .setResponses(Collections.singletonList(
                new FetchResponseData.FetchableTopicResponse().setTopic(topic).setTopicId(topicId)
                    .setPartitions(Collections.singletonList(new FetchResponseData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition), topicId)
        );

        List<FetchResponseData> invalidRequestResponses = Arrays.asList(
            new FetchResponseData(),
            new FetchResponseData().setResponses(
                Arrays.asList(
                    new FetchResponseData.FetchableTopicResponse(),
                    new FetchResponseData.FetchableTopicResponse())),
            new FetchResponseData().setResponses(
                Collections.singletonList(
                    new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topic)
                        .setTopicId(topicId)
                )
            ),
            new FetchResponseData().setResponses(
                Collections.singletonList(
                    new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topic)
                        .setTopicId(topicId)
                        .setPartitions(
                            Arrays.asList(
                                new FetchResponseData.PartitionData(),
                                new FetchResponseData.PartitionData()
                            )
                        )
                )
            )
        );
        for (FetchResponseData invalidRequest : invalidRequestResponses) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition), topicId)
            );
        }

        List<FetchResponseData> unknownTopicOrPartitionResponses = Arrays.asList(
            new FetchResponseData().setResponses(
                Collections.singletonList(
                    new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topic)
                        .setTopicId(Uuid.randomUuid())
                        .setPartitions(
                            Collections.singletonList(new FetchResponseData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new FetchResponseData().setResponses(
                Collections.singletonList(
                    new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topic)
                        .setTopicId(topicId)
                        .setPartitions(
                            Collections.singletonList(new FetchResponseData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (FetchResponseData unknownTopicOrPartition: unknownTopicOrPartitionResponses) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartition, new TopicPartition(topic, partition), topicId)
            );
        }
    }

    @Test
    public void testValidateVoteRequestData() {
        VoteRequestData none = new VoteRequestData()
            .setTopics(Collections.singletonList(
                new VoteRequestData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new VoteRequestData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<VoteRequestData> invalidRequests = Arrays.asList(
            new VoteRequestData(),
            new VoteRequestData().setTopics(
                Arrays.asList(
                    new VoteRequestData.TopicData(),
                    new VoteRequestData.TopicData())),
            new VoteRequestData().setTopics(
                Collections.singletonList(
                    new VoteRequestData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new VoteRequestData().setTopics(
                Collections.singletonList(
                    new VoteRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new VoteRequestData.PartitionData(),
                                new VoteRequestData.PartitionData()
                            )
                        )
                )
            )
        );
        for (VoteRequestData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<VoteRequestData> unknownTopicOrPartitionRequests = Arrays.asList(
            new VoteRequestData().setTopics(
                Collections.singletonList(
                    new VoteRequestData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new VoteRequestData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new VoteRequestData().setTopics(
                Collections.singletonList(
                    new VoteRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new VoteRequestData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (VoteRequestData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateVoteResponseData() {
        VoteResponseData none = new VoteResponseData()
            .setTopics(Collections.singletonList(
                new VoteResponseData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new VoteResponseData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<VoteResponseData> invalidRequests = Arrays.asList(
            new VoteResponseData(),
            new VoteResponseData().setTopics(
                Arrays.asList(
                    new VoteResponseData.TopicData(),
                    new VoteResponseData.TopicData())),
            new VoteResponseData().setTopics(
                Collections.singletonList(
                    new VoteResponseData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new VoteResponseData().setTopics(
                Collections.singletonList(
                    new VoteResponseData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new VoteResponseData.PartitionData(),
                                new VoteResponseData.PartitionData()
                            )
                        )
                )
            )
        );
        for (VoteResponseData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<VoteResponseData> unknownTopicOrPartitionRequests = Arrays.asList(
            new VoteResponseData().setTopics(
                Collections.singletonList(
                    new VoteResponseData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new VoteResponseData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new VoteResponseData().setTopics(
                Collections.singletonList(
                    new VoteResponseData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new VoteResponseData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (VoteResponseData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateBeginQuorumEpochRequestData() {
        BeginQuorumEpochRequestData none = new BeginQuorumEpochRequestData()
            .setTopics(Collections.singletonList(
                new BeginQuorumEpochRequestData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new BeginQuorumEpochRequestData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<BeginQuorumEpochRequestData> invalidRequests = Arrays.asList(
            new BeginQuorumEpochRequestData(),
            new BeginQuorumEpochRequestData().setTopics(
                Arrays.asList(
                    new BeginQuorumEpochRequestData.TopicData(),
                    new BeginQuorumEpochRequestData.TopicData())),
            new BeginQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochRequestData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new BeginQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new BeginQuorumEpochRequestData.PartitionData(),
                                new BeginQuorumEpochRequestData.PartitionData()
                            )
                        )
                )
            )
        );
        for (BeginQuorumEpochRequestData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<BeginQuorumEpochRequestData> unknownTopicOrPartitionRequests = Arrays.asList(
            new BeginQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochRequestData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new BeginQuorumEpochRequestData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new BeginQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new BeginQuorumEpochRequestData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (BeginQuorumEpochRequestData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateBeginQuorumEpochResponseData() {
        BeginQuorumEpochResponseData none = new BeginQuorumEpochResponseData()
            .setTopics(Collections.singletonList(
                new BeginQuorumEpochResponseData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new BeginQuorumEpochResponseData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<BeginQuorumEpochResponseData> invalidRequests = Arrays.asList(
            new BeginQuorumEpochResponseData(),
            new BeginQuorumEpochResponseData().setTopics(
                Arrays.asList(
                    new BeginQuorumEpochResponseData.TopicData(),
                    new BeginQuorumEpochResponseData.TopicData())),
            new BeginQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochResponseData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new BeginQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochResponseData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new BeginQuorumEpochResponseData.PartitionData(),
                                new BeginQuorumEpochResponseData.PartitionData()
                            )
                        )
                )
            )
        );
        for (BeginQuorumEpochResponseData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<BeginQuorumEpochResponseData> unknownTopicOrPartitionRequests = Arrays.asList(
            new BeginQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochResponseData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new BeginQuorumEpochResponseData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new BeginQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochResponseData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new BeginQuorumEpochResponseData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (BeginQuorumEpochResponseData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateEndQuorumEpochRequestData() {
        EndQuorumEpochRequestData none = new EndQuorumEpochRequestData()
            .setTopics(Collections.singletonList(
                new EndQuorumEpochRequestData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new EndQuorumEpochRequestData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<EndQuorumEpochRequestData> invalidRequests = Arrays.asList(
            new EndQuorumEpochRequestData(),
            new EndQuorumEpochRequestData().setTopics(
                Arrays.asList(
                    new EndQuorumEpochRequestData.TopicData(),
                    new EndQuorumEpochRequestData.TopicData())),
            new EndQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new EndQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new EndQuorumEpochRequestData.PartitionData(),
                                new EndQuorumEpochRequestData.PartitionData()
                            )
                        )
                )
            )
        );
        for (EndQuorumEpochRequestData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<EndQuorumEpochRequestData> unknownTopicOrPartitionRequests = Arrays.asList(
            new EndQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new EndQuorumEpochRequestData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new EndQuorumEpochRequestData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new EndQuorumEpochRequestData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (EndQuorumEpochRequestData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateEndQuorumEpochResponseData() {
        EndQuorumEpochResponseData none = new EndQuorumEpochResponseData()
            .setTopics(Collections.singletonList(
                new EndQuorumEpochResponseData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new EndQuorumEpochResponseData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<EndQuorumEpochResponseData> invalidRequests = Arrays.asList(
            new EndQuorumEpochResponseData(),
            new EndQuorumEpochResponseData().setTopics(
                Arrays.asList(
                    new EndQuorumEpochResponseData.TopicData(),
                    new EndQuorumEpochResponseData.TopicData())),
            new EndQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochResponseData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new EndQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochResponseData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new EndQuorumEpochResponseData.PartitionData(),
                                new EndQuorumEpochResponseData.PartitionData()
                            )
                        )
                )
            )
        );
        for (EndQuorumEpochResponseData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<EndQuorumEpochResponseData> unknownTopicOrPartitionRequests = Arrays.asList(
            new EndQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochResponseData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new EndQuorumEpochResponseData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new EndQuorumEpochResponseData().setTopics(
                Collections.singletonList(
                    new EndQuorumEpochResponseData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new EndQuorumEpochResponseData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (EndQuorumEpochResponseData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateDescribeQuorumRequestData() {
        DescribeQuorumRequestData none = new DescribeQuorumRequestData()
            .setTopics(Collections.singletonList(
                new DescribeQuorumRequestData.TopicData().setTopicName(topic)
                    .setPartitions(Collections.singletonList(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<DescribeQuorumRequestData> invalidRequests = Arrays.asList(
            new DescribeQuorumRequestData(),
            new DescribeQuorumRequestData().setTopics(
                Arrays.asList(
                    new DescribeQuorumRequestData.TopicData(),
                    new DescribeQuorumRequestData.TopicData())),
            new DescribeQuorumRequestData().setTopics(
                Collections.singletonList(
                    new DescribeQuorumRequestData.TopicData()
                        .setTopicName(topic)
                )
            ),
            new DescribeQuorumRequestData().setTopics(
                Collections.singletonList(
                    new DescribeQuorumRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new DescribeQuorumRequestData.PartitionData(),
                                new DescribeQuorumRequestData.PartitionData()
                            )
                        )
                )
            )
        );
        for (DescribeQuorumRequestData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<DescribeQuorumRequestData> unknownTopicOrPartitionRequests = Arrays.asList(
            new DescribeQuorumRequestData().setTopics(
                Collections.singletonList(
                    new DescribeQuorumRequestData.TopicData()
                        .setTopicName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(0))
                        )
                )
            ),
            new DescribeQuorumRequestData().setTopics(
                Collections.singletonList(
                    new DescribeQuorumRequestData.TopicData()
                        .setTopicName(topic)
                        .setPartitions(
                            Collections.singletonList(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(1))
                        )
                )
            )
        );
        for (DescribeQuorumRequestData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateFetchSnapshotRequestData() {
        FetchSnapshotRequestData none = new FetchSnapshotRequestData()
            .setTopics(Collections.singletonList(
                new FetchSnapshotRequestData.TopicSnapshot().setName(topic)
                    .setPartitions(Collections.singletonList(new FetchSnapshotRequestData.PartitionSnapshot().setPartition(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<FetchSnapshotRequestData> invalidRequests = Arrays.asList(
            new FetchSnapshotRequestData(),
            new FetchSnapshotRequestData().setTopics(
                Arrays.asList(
                    new FetchSnapshotRequestData.TopicSnapshot(),
                    new FetchSnapshotRequestData.TopicSnapshot())),
            new FetchSnapshotRequestData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topic)
                )
            ),
            new FetchSnapshotRequestData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new FetchSnapshotRequestData.PartitionSnapshot(),
                                new FetchSnapshotRequestData.PartitionSnapshot()
                            )
                        )
                )
            )
        );
        for (FetchSnapshotRequestData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<FetchSnapshotRequestData> unknownTopicOrPartitionRequests = Arrays.asList(
            new FetchSnapshotRequestData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new FetchSnapshotRequestData.PartitionSnapshot().setPartition(0))
                        )
                )
            ),
            new FetchSnapshotRequestData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topic)
                        .setPartitions(
                            Collections.singletonList(new FetchSnapshotRequestData.PartitionSnapshot().setPartition(1))
                        )
                )
            )
        );
        for (FetchSnapshotRequestData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }

    @Test
    public void testValidateFetchSnapshotResponseData() {
        FetchSnapshotResponseData none = new FetchSnapshotResponseData()
            .setTopics(Collections.singletonList(
                new FetchSnapshotResponseData.TopicSnapshot().setName(topic)
                    .setPartitions(Collections.singletonList(new FetchSnapshotResponseData.PartitionSnapshot().setIndex(partition)))));
        assertEquals(
            Errors.NONE,
            RaftUtil.validateTopicPartition(none, new TopicPartition(topic, partition))
        );

        List<FetchSnapshotResponseData> invalidRequests = Arrays.asList(
            new FetchSnapshotResponseData(),
            new FetchSnapshotResponseData().setTopics(
                Arrays.asList(
                    new FetchSnapshotResponseData.TopicSnapshot(),
                    new FetchSnapshotResponseData.TopicSnapshot())),
            new FetchSnapshotResponseData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topic)
                )
            ),
            new FetchSnapshotResponseData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topic)
                        .setPartitions(
                            Arrays.asList(
                                new FetchSnapshotResponseData.PartitionSnapshot(),
                                new FetchSnapshotResponseData.PartitionSnapshot()
                            )
                        )
                )
            )
        );
        for (FetchSnapshotResponseData invalidRequest : invalidRequests) {
            assertEquals(
                Errors.INVALID_REQUEST,
                RaftUtil.validateTopicPartition(invalidRequest, new TopicPartition(topic, partition))
            );
        }

        List<FetchSnapshotResponseData> unknownTopicOrPartitionRequests = Arrays.asList(
            new FetchSnapshotResponseData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topic + "1")
                        .setPartitions(
                            Collections.singletonList(new FetchSnapshotResponseData.PartitionSnapshot().setIndex(0))
                        )
                )
            ),
            new FetchSnapshotResponseData().setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topic)
                        .setPartitions(
                            Collections.singletonList(new FetchSnapshotResponseData.PartitionSnapshot().setIndex(1))
                        )
                )
            )
        );
        for (FetchSnapshotResponseData unknownTopicOrPartitionRequest: unknownTopicOrPartitionRequests) {
            assertEquals(
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                RaftUtil.validateTopicPartition(unknownTopicOrPartitionRequest, new TopicPartition(topic, partition))
            );
        }
    }
}
