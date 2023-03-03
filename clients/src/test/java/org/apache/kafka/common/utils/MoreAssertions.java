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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class MoreAssertions {
    /**
     * Compares the two {@link OffsetCommitRequestData} independently of the order in which the
     * {@link OffsetCommitRequestTopic} and {@link OffsetCommitRequestPartition} are defined in the response.
     */
    public static void assertRequestEquals(OffsetCommitRequestData expected, OffsetCommitRequestData actual) {
        assertEquals(expected.groupId(), actual.groupId(), "Group id mismatch");
        assertEquals(expected.groupInstanceId(), actual.groupInstanceId(), "Group instance id mismatch");
        assertEquals(expected.generationId(), actual.generationId(), "Generation id mismatch");
        assertEquals(expected.memberId(), actual.memberId(), "Member id mismatch");
        assertEquals(expected.retentionTimeMs(), actual.retentionTimeMs(), "Retention time mismatch");
        assertEquals(offsetCommitRequestPartitions(expected), offsetCommitRequestPartitions(actual));
    }

    private static Map<TopicIdPartition, OffsetCommitRequestPartition> offsetCommitRequestPartitions(
            OffsetCommitRequestData request) {
        return request.topics().stream()
            .flatMap(topic -> topic.partitions().stream()
                .collect(Collectors.toMap(
                    p -> new TopicIdPartition(topic.topicId(), p.partitionIndex(), topic.name()),
                    identity()))
                .entrySet()
                .stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private MoreAssertions() {
    }
}
