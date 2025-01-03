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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.storage.log.FetchParams;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ShareFetchTest {

    private static final String GROUP_ID = "groupId";
    private static final String MEMBER_ID = "memberId";

    @Test
    public void testErrorInAllPartitions() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, new CompletableFuture<>(),
            Map.of(topicIdPartition, 10), 100);
        assertFalse(shareFetch.errorInAllPartitions());

        shareFetch.addErroneous(topicIdPartition, new RuntimeException());
        assertTrue(shareFetch.errorInAllPartitions());
    }

    @Test
    public void testErrorInAllPartitionsWithMultipleTopicIdPartitions() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, new CompletableFuture<>(),
            Map.of(topicIdPartition0, 10, topicIdPartition1, 10), 100);
        assertFalse(shareFetch.errorInAllPartitions());

        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        assertFalse(shareFetch.errorInAllPartitions());

        shareFetch.addErroneous(topicIdPartition1, new RuntimeException());
        assertTrue(shareFetch.errorInAllPartitions());
    }

    @Test
    public void testFilterErroneousTopicPartitions() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, new CompletableFuture<>(),
            Map.of(topicIdPartition0, 10, topicIdPartition1, 10), 100);
        Set<TopicIdPartition> result = shareFetch.filterErroneousTopicPartitions(Set.of(topicIdPartition0, topicIdPartition1));
        // No erroneous partitions, hence all partitions should be returned.
        assertEquals(2, result.size());
        assertTrue(result.contains(topicIdPartition0));
        assertTrue(result.contains(topicIdPartition1));

        // Add an erroneous partition and verify that it is filtered out.
        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        result = shareFetch.filterErroneousTopicPartitions(Set.of(topicIdPartition0, topicIdPartition1));
        assertEquals(1, result.size());
        assertTrue(result.contains(topicIdPartition1));

        // Add another erroneous partition and verify that it is filtered out.
        shareFetch.addErroneous(topicIdPartition1, new RuntimeException());
        result = shareFetch.filterErroneousTopicPartitions(Set.of(topicIdPartition0, topicIdPartition1));
        assertTrue(result.isEmpty());
    }

}
