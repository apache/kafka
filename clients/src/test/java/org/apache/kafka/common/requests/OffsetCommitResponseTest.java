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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicResolver;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetCommitResponseTest {

    protected final int throttleTimeMs = 10;

    protected final String topicOne = "topic1";
    protected final Uuid topic1Id = Uuid.randomUuid();
    protected final int partitionOne = 1;
    protected final Errors errorOne = Errors.COORDINATOR_NOT_AVAILABLE;
    protected final Errors errorTwo = Errors.NOT_COORDINATOR;
    protected final String topicTwo = "topic2";
    protected final int partitionTwo = 2;

    protected TopicPartition tp1 = new TopicPartition(topicOne, partitionOne);
    protected TopicPartition tp2 = new TopicPartition(topicTwo, partitionTwo);
    protected Map<Errors, Integer> expectedErrorCounts;
    protected Map<TopicPartition, Errors> errorsMap;

    @BeforeEach
    public void setUp() {
        expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(errorOne, 1);
        expectedErrorCounts.put(errorTwo, 1);

        errorsMap = new HashMap<>();
        errorsMap.put(tp1, errorOne);
        errorsMap.put(tp2, errorTwo);
    }

    @Test
    public void testConstructorWithErrorResponse() {
        OffsetCommitResponse response = new OffsetCommitResponse(
                throttleTimeMs, errorsMap, OffsetCommitResponseData.HIGHEST_SUPPORTED_VERSION);

        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testParse(short version) {
        OffsetCommitResponseData data = new OffsetCommitResponseData()
            .setTopics(Arrays.asList(
                new OffsetCommitResponseTopic().setPartitions(
                    Collections.singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionOne)
                        .setErrorCode(errorOne.code()))),
                new OffsetCommitResponseTopic().setPartitions(
                    Collections.singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionTwo)
                        .setErrorCode(errorTwo.code())))
            ))
            .setThrottleTimeMs(throttleTimeMs);

        ByteBuffer buffer = MessageUtil.toByteBuffer(data, version);
        OffsetCommitResponse response = OffsetCommitResponse.parse(buffer, version);
        assertEquals(expectedErrorCounts, response.errorCounts());

        if (version >= 3) {
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        } else {
            assertEquals(DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
        }

        assertEquals(version >= 4, response.shouldClientThrottle(version));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testOffsetCommitResponseBuilder(short version) {
        NameAndId topic3 = new NameAndId("topic3");
        NameAndId topic4 = new NameAndId("topic4");
        NameAndId topic5 = new NameAndId("topic5");
        NameAndId topic6 = new NameAndId("topic6");

        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(topicOne, topic1Id);
        asList(topic3, topic4, topic5, topic6).forEach(nai -> topicIds.put(nai.name, nai.id));

        TopicResolver resolver = TopicResolver.fromTopicIds(topicIds);

        OffsetCommitResponse.Builder<?> builder = OffsetCommitResponse.newBuilder(resolver, version)
            .addPartition(topicOne, topic1Id, partitionOne, Errors.NONE)
            .addPartition(topicOne, topic1Id, partitionTwo, Errors.NONE)
            .addPartitions(topic6.name, topic6.id, asList(11, 12), identity(), Errors.NONE);

        List<OffsetCommitResponseTopic> expectedTopics = new ArrayList<>();

        if (version < 9) {
            builder.addPartition(topicTwo, Uuid.ZERO_UUID, 3, Errors.NONE)
                .addPartition(topicTwo, Uuid.ZERO_UUID, 4, Errors.NONE)
                .addPartition(topic3.name, Uuid.ZERO_UUID, 5, Errors.NONE)
                .addPartition(topic3.name, Uuid.ZERO_UUID, 6, Errors.NONE);

            expectedTopics.addAll(asList(
                createResponseTopic(topicOne, topic1Id, partitionOne, partitionTwo, Errors.NONE),
                createResponseTopic(topic6.name, topic6.id, 11, 12, Errors.NONE),
                createResponseTopic(topicTwo, Uuid.ZERO_UUID, 3, 4, Errors.NONE),
                createResponseTopic(topic3.name, Uuid.ZERO_UUID, 5, 6, Errors.NONE)
            ));

        } else {
            builder.addPartition(null, topic4.id, 7, Errors.NONE)
                .addPartition(null, topic4.id, 8, Errors.NONE)
                .addPartition("", topic5.id, 9, Errors.NONE)
                .addPartition("", topic5.id, 10, Errors.NONE)
                .addPartition(topicTwo, Uuid.ZERO_UUID, 3, Errors.NONE);

            expectedTopics.addAll(asList(
                createResponseTopic(topicOne, topic1Id, partitionOne, partitionTwo, Errors.NONE),
                createResponseTopic(topic6.name, topic6.id, 11, 12, Errors.NONE),
                createResponseTopic(null, topic4.id, 7, 8, Errors.NONE),
                createResponseTopic("", topic5.id, 9, 10, Errors.NONE),
                createResponseTopic(topicTwo, Uuid.ZERO_UUID, 3, Errors.NONE)
            ));
        }

        assertEquals(new OffsetCommitResponseData().setTopics(expectedTopics), builder.build().data());
    }

    private static OffsetCommitResponseTopic createResponseTopic(
            String topicName, Uuid topicId, int partition, Errors error) {
        return new OffsetCommitResponseTopic()
            .setTopicId(topicId)
            .setName(topicName)
            .setPartitions(new ArrayList<>(asList(
                new OffsetCommitResponsePartition()
                    .setPartitionIndex(partition)
                    .setErrorCode(error.code())
            )));
    }

    private static OffsetCommitResponseTopic createResponseTopic(
            String topicName, Uuid topicId, int firstPartition, int secondPartition, Errors error) {
        OffsetCommitResponseTopic topic = createResponseTopic(topicName, topicId, firstPartition, error);
        topic.partitions().add(new OffsetCommitResponsePartition()
            .setPartitionIndex(secondPartition)
            .setErrorCode(error.code()));

        return topic;
    }

    public static final class NameAndId {
        private final String name;
        private final Uuid id;

        NameAndId(String name) {
            this.name = name;
            this.id = Uuid.randomUuid();
        }

        public NameAndId(String name, Uuid id) {
            this.name = name;
            this.id = id;
        }

        public String name() {
            return name;
        }

        public Uuid id() {
            return id;
        }
    }
}
