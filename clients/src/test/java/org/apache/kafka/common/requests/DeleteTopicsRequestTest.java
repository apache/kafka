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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.ApiKeys.DELETE_TOPICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteTopicsRequestTest {

    @Test
    public void testTopicNormalization() {
        for (short version : DELETE_TOPICS.allVersions()) {
            // Check topic names are in the correct place when using topicNames.
            String topic1 = "topic1";
            String topic2 = "topic2";
            List<String> topics = Arrays.asList(topic1, topic2);
            DeleteTopicsRequest requestWithNames = new DeleteTopicsRequest.Builder(
                    new DeleteTopicsRequestData().setTopicNames(topics)).build(version);
            DeleteTopicsRequest requestWithNamesSerialized = DeleteTopicsRequest.parse(requestWithNames.serialize(), version);

            assertEquals(topics, requestWithNames.topicNames());
            assertEquals(topics, requestWithNamesSerialized.topicNames());

            if (version < 6) {
                assertEquals(topics, requestWithNames.data().topicNames());
                assertEquals(topics, requestWithNamesSerialized.data().topicNames());
            } else {
                // topics in TopicNames are moved to new topics field
                assertEquals(topics, requestWithNames.data().topics().stream().map(DeleteTopicState::name).collect(Collectors.toList()));
                assertEquals(topics, requestWithNamesSerialized.data().topics().stream().map(DeleteTopicState::name).collect(Collectors.toList()));
            }
        }
    }

    @Test
    public void testNewTopicsField() {
        for (short version : DELETE_TOPICS.allVersions()) {
            String topic1 = "topic1";
            String topic2 = "topic2";
            List<String> topics = Arrays.asList(topic1, topic2);
            DeleteTopicsRequest requestWithNames = new DeleteTopicsRequest.Builder(
                    new DeleteTopicsRequestData().setTopics(Arrays.asList(
                            new DeleteTopicsRequestData.DeleteTopicState().setName(topic1),
                            new DeleteTopicsRequestData.DeleteTopicState().setName(topic2)))).build(version);
            // Ensure we only use new topics field on versions 6+.
            if (version >= 6) {
                DeleteTopicsRequest requestWithNamesSerialized = DeleteTopicsRequest.parse(requestWithNames.serialize(), version);

                assertEquals(topics, requestWithNames.topicNames());
                assertEquals(topics, requestWithNamesSerialized.topicNames());

            } else {
                // We should fail if version is less than 6.
                assertThrows(UnsupportedVersionException.class, () -> requestWithNames.serialize());
            }
        }
    }

    @Test
    public void testTopicIdsField() {
        for (short version : DELETE_TOPICS.allVersions()) {
            // Check topic IDs are handled correctly. We should only use this field on versions 6+.
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            List<Uuid> topicIds = Arrays.asList(topicId1, topicId2);
            DeleteTopicsRequest requestWithIds = new DeleteTopicsRequest.Builder(
                    new DeleteTopicsRequestData().setTopics(Arrays.asList(
                            new DeleteTopicsRequestData.DeleteTopicState().setTopicId(topicId1),
                            new DeleteTopicsRequestData.DeleteTopicState().setTopicId(topicId2)))).build(version);

            if (version >= 6) {
                DeleteTopicsRequest requestWithIdsSerialized = DeleteTopicsRequest.parse(requestWithIds.serialize(), version);

                assertEquals(topicIds, requestWithIds.topicIds());
                assertEquals(topicIds, requestWithIdsSerialized.topicIds());

                // All topic names should be replaced with null
                requestWithIds.data().topics().forEach(topic -> assertNull(topic.name()));
                requestWithIdsSerialized.data().topics().forEach(topic -> assertNull(topic.name()));
            } else {
                // We should fail if version is less than 6.
                assertThrows(UnsupportedVersionException.class, () -> requestWithIds.serialize());
            }
        }
    }

    @Test
    public void testDeleteTopicsRequestNumTopics() {
        for (short version : DELETE_TOPICS.allVersions()) {
            DeleteTopicsRequest request = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
                    .setTopicNames(Arrays.asList("topic1", "topic2"))
                    .setTimeoutMs(1000)).build(version);
            DeleteTopicsRequest serializedRequest = DeleteTopicsRequest.parse(request.serialize(), version);
            // createDeleteTopicsRequest sets 2 topics
            assertEquals(2, request.numberOfTopics());
            assertEquals(2, serializedRequest.numberOfTopics());

            // Test using IDs
            if (version >= 6) {
                DeleteTopicsRequest requestWithIds = new DeleteTopicsRequest.Builder(
                        new DeleteTopicsRequestData().setTopics(Arrays.asList(
                                new DeleteTopicsRequestData.DeleteTopicState().setTopicId(Uuid.randomUuid()),
                                new DeleteTopicsRequestData.DeleteTopicState().setTopicId(Uuid.randomUuid())))).build(version);
                DeleteTopicsRequest serializedRequestWithIds = DeleteTopicsRequest.parse(requestWithIds.serialize(), version);
                assertEquals(2, requestWithIds.numberOfTopics());
                assertEquals(2, serializedRequestWithIds.numberOfTopics());
            }
        }
    }
}
