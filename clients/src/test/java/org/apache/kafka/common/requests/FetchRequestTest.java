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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FetchRequestTest {

    private static Stream<Arguments> fetchVersions() {
        return ApiKeys.FETCH.allVersions().stream().map(version -> Arguments.of(version));
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testToReplaceWithDifferentVersions(short version) {
        boolean fetchRequestUsesTopicIds = version >= 13;
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp = new TopicIdPartition(topicId, 0, "topic");

        Map<TopicPartition, FetchRequest.PartitionData> partitionData = Collections.singletonMap(tp.topicPartition(),
                new FetchRequest.PartitionData(topicId, 0, 0, 0, Optional.empty()));
        List<TopicIdPartition> toReplace = Collections.singletonList(tp);

        FetchRequest fetchRequest = FetchRequest.Builder
                .forReplica(version, 0, 1, 1, 1, partitionData)
                .removed(Collections.emptyList())
                .replaced(toReplace)
                .metadata(FetchMetadata.newIncremental(123)).build(version);

        // If version < 13, we should not see any partitions in forgottenTopics. This is because we can not
        // distinguish different topic IDs on versions earlier than 13.
        assertEquals(fetchRequestUsesTopicIds, fetchRequest.data().forgottenTopicsData().size() > 0);
        fetchRequest.data().forgottenTopicsData().forEach(forgottenTopic -> {
            // Since we didn't serialize, we should see the topic name and ID regardless of the version.
            assertEquals(tp.topic(), forgottenTopic.topic());
            assertEquals(topicId, forgottenTopic.topicId());
        });

        assertEquals(1, fetchRequest.data().topics().size());
        fetchRequest.data().topics().forEach(topic -> {
            // Since we didn't serialize, we should see the topic name and ID regardless of the version.
            assertEquals(tp.topic(), topic.topic());
            assertEquals(topicId, topic.topicId());
        });
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testFetchData(short version) {
        TopicPartition topicPartition0 = new TopicPartition("topic", 0);
        TopicPartition topicPartition1 = new TopicPartition("unknownIdTopic", 0);
        Uuid topicId0 = Uuid.randomUuid();
        Uuid topicId1 = Uuid.randomUuid();

        // Only include topic IDs for the first topic partition.
        Map<Uuid, String> topicNames = Collections.singletonMap(topicId0, topicPartition0.topic());
        List<TopicIdPartition> topicIdPartitions = new LinkedList<>();
        topicIdPartitions.add(new TopicIdPartition(topicId0, topicPartition0));
        topicIdPartitions.add(new TopicIdPartition(topicId1, topicPartition1));

        // Include one topic with topic IDs in the topic names map and one without.
        Map<TopicPartition, FetchRequest.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(topicPartition0, new FetchRequest.PartitionData(topicId0, 0, 0, 0, Optional.empty()));
        partitionData.put(topicPartition1, new FetchRequest.PartitionData(topicId1, 0, 0, 0, Optional.empty()));
        boolean fetchRequestUsesTopicIds = version >= 13;

        FetchRequest fetchRequest = FetchRequest.parse(FetchRequest.Builder
                .forReplica(version, 0, 1, 1, 1, partitionData)
                .removed(Collections.emptyList())
                .replaced(Collections.emptyList())
                .metadata(FetchMetadata.newIncremental(123)).build(version).serialize(), version);

        if (version >= 15) {
            assertEquals(1, fetchRequest.data().replicaState().replicaEpoch());
        }
        // For versions < 13, we will be provided a topic name and a zero UUID in FetchRequestData.
        // Versions 13+ will contain a valid topic ID but an empty topic name.
        List<TopicIdPartition> expectedData = new LinkedList<>();
        topicIdPartitions.forEach(tidp -> {
            String expectedName = fetchRequestUsesTopicIds ? "" : tidp.topic();
            Uuid expectedTopicId = fetchRequestUsesTopicIds ? tidp.topicId() : Uuid.ZERO_UUID;
            expectedData.add(new TopicIdPartition(expectedTopicId, tidp.partition(), expectedName));
        });

        // Build the list of TopicIdPartitions based on the FetchRequestData that was serialized and parsed.
        List<TopicIdPartition> convertedFetchData = new LinkedList<>();
        fetchRequest.data().topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                        convertedFetchData.add(new TopicIdPartition(topic.topicId(), partition.partition(), topic.topic()))
                )
        );
        // The TopicIdPartitions built from the request data should match what we expect.
        assertEquals(expectedData, convertedFetchData);

        // For fetch request version 13+ we expect topic names to be filled in for all topics in the topicNames map.
        // Otherwise, the topic name should be null.
        // For earlier request versions, we expect topic names and zero Uuids.
        Map<TopicIdPartition, FetchRequest.PartitionData> expectedFetchData = new LinkedHashMap<>();
        // Build the expected map based on fetchRequestUsesTopicIds.
        expectedData.forEach(tidp -> {
            String expectedName = fetchRequestUsesTopicIds ? topicNames.get(tidp.topicId()) : tidp.topic();
            TopicIdPartition tpKey = new TopicIdPartition(tidp.topicId(), new TopicPartition(expectedName, tidp.partition()));
            // logStartOffset was not a valid field in versions 4 and earlier.
            int logStartOffset = version > 4 ? 0 : -1;
            expectedFetchData.put(tpKey, new FetchRequest.PartitionData(tidp.topicId(), 0, logStartOffset, 0, Optional.empty()));
        });
        assertEquals(expectedFetchData, fetchRequest.fetchData(topicNames));
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testForgottenTopics(short version) {
        // Forgotten topics are not allowed prior to version 7
        if (version >= 7) {
            TopicPartition topicPartition0 = new TopicPartition("topic", 0);
            TopicPartition topicPartition1 = new TopicPartition("unknownIdTopic", 0);
            Uuid topicId0 = Uuid.randomUuid();
            Uuid topicId1 = Uuid.randomUuid();
            // Only include topic IDs for the first topic partition.
            Map<Uuid, String> topicNames = Collections.singletonMap(topicId0, topicPartition0.topic());

            // Include one topic with topic IDs in the topic names map and one without.
            List<TopicIdPartition> toForgetTopics = new LinkedList<>();
            toForgetTopics.add(new TopicIdPartition(topicId0, topicPartition0));
            toForgetTopics.add(new TopicIdPartition(topicId1, topicPartition1));

            boolean fetchRequestUsesTopicIds = version >= 13;

            FetchRequest fetchRequest = FetchRequest.parse(FetchRequest.Builder
                    .forReplica(version, 0, 1, 1, 1, Collections.emptyMap())
                    .removed(toForgetTopics)
                    .replaced(Collections.emptyList())
                    .metadata(FetchMetadata.newIncremental(123)).build(version).serialize(), version);

            // For versions < 13, we will be provided a topic name and a zero Uuid in FetchRequestData.
            // Versions 13+ will contain a valid topic ID but an empty topic name.
            List<TopicIdPartition> expectedForgottenTopicData = new LinkedList<>();
            toForgetTopics.forEach(tidp -> {
                String expectedName = fetchRequestUsesTopicIds ? "" : tidp.topic();
                Uuid expectedTopicId = fetchRequestUsesTopicIds ? tidp.topicId() : Uuid.ZERO_UUID;
                expectedForgottenTopicData.add(new TopicIdPartition(expectedTopicId, tidp.partition(), expectedName));
            });

            // Build the list of TopicIdPartitions based on the FetchRequestData that was serialized and parsed.
            List<TopicIdPartition> convertedForgottenTopicData = new LinkedList<>();
            fetchRequest.data().forgottenTopicsData().forEach(forgottenTopic ->
                    forgottenTopic.partitions().forEach(partition ->
                            convertedForgottenTopicData.add(new TopicIdPartition(forgottenTopic.topicId(), partition, forgottenTopic.topic()))
                    )
            );
            // The TopicIdPartitions built from the request data should match what we expect.
            assertEquals(expectedForgottenTopicData, convertedForgottenTopicData);

            // Get the forgottenTopics from the request data.
            List<TopicIdPartition> forgottenTopics = fetchRequest.forgottenTopics(topicNames);

            // For fetch request version 13+ we expect topic names to be filled in for all topics in the topicNames map.
            // Otherwise, the topic name should be null.
            // For earlier request versions, we expect topic names and zero Uuids.
            // Build the list of expected TopicIdPartitions. These are different from the earlier expected topicIdPartitions
            // as empty strings are converted to nulls.
            assertEquals(expectedForgottenTopicData.size(), forgottenTopics.size());
            List<TopicIdPartition> expectedForgottenTopics = new LinkedList<>();
            expectedForgottenTopicData.forEach(tidp -> {
                String expectedName = fetchRequestUsesTopicIds ? topicNames.get(tidp.topicId()) : tidp.topic();
                expectedForgottenTopics.add(new TopicIdPartition(tidp.topicId(), new TopicPartition(expectedName, tidp.partition())));
            });
            assertEquals(expectedForgottenTopics, forgottenTopics);
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FETCH)
    public void testFetchRequestSimpleBuilderReplicaStateDowngrade(short version) {
        FetchRequestData fetchRequestData = new FetchRequestData();
        fetchRequestData.setReplicaState(new FetchRequestData.ReplicaState().setReplicaId(1));
        FetchRequest.SimpleBuilder builder = new FetchRequest.SimpleBuilder(fetchRequestData);
        fetchRequestData = builder.build(version).data();

        assertEquals(1, FetchRequest.replicaId(fetchRequestData));

        if (version < 15) {
            assertEquals(1, fetchRequestData.replicaId());
            assertEquals(-1, fetchRequestData.replicaState().replicaId());
        } else {
            assertEquals(-1, fetchRequestData.replicaId());
            assertEquals(1, fetchRequestData.replicaState().replicaId());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FETCH)
    public void testFetchRequestSimpleBuilderReplicaIdNotSupported(short version) {
        FetchRequestData fetchRequestData = new FetchRequestData().setReplicaId(1);
        FetchRequest.SimpleBuilder builder = new FetchRequest.SimpleBuilder(fetchRequestData);
        assertThrows(IllegalStateException.class, () -> {
            builder.build(version);
        });
    }

    @Test
    public void testPartitionDataEquals() {
        assertEquals(new FetchRequest.PartitionData(Uuid.ZERO_UUID, 300, 0L, 300, Optional.of(300)),
                new FetchRequest.PartitionData(Uuid.ZERO_UUID, 300, 0L, 300, Optional.of(300)));

        assertNotEquals(new FetchRequest.PartitionData(Uuid.randomUuid(), 300, 0L, 300, Optional.of(300)),
            new FetchRequest.PartitionData(Uuid.randomUuid(), 300, 0L, 300, Optional.of(300)));
    }

}
