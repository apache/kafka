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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteConsumerGroupOffsetsResultTest {

    private final String topic = "topic";
    private final TopicPartition tpZero = new TopicPartition(topic, 0);
    private final TopicPartition tpOne = new TopicPartition(topic, 1);
    private Set<TopicPartition> partitions;
    private Map<TopicPartition, Errors> errorsMap;

    private KafkaFutureImpl<Map<TopicPartition, Errors>> partitionFutures;

    @BeforeEach
    public void setUp() {
        partitionFutures = new KafkaFutureImpl<>();
        partitions = new HashSet<>();
        partitions.add(tpZero);
        partitions.add(tpOne);

        errorsMap = new HashMap<>();
        errorsMap.put(tpZero, Errors.NONE);
        errorsMap.put(tpOne, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testTopLevelErrorConstructor() throws InterruptedException {
        partitionFutures.completeExceptionally(Errors.GROUP_AUTHORIZATION_FAILED.exception());
        DeleteConsumerGroupOffsetsResult topLevelErrorResult =
            new DeleteConsumerGroupOffsetsResult(partitionFutures, partitions);
        TestUtils.assertFutureError(topLevelErrorResult.all(), GroupAuthorizationException.class);
    }

    @Test
    public void testPartitionLevelErrorConstructor() throws ExecutionException, InterruptedException {
        createAndVerifyPartitionLevelErrror();
    }

    @Test
    public void testPartitionMissingInResponseErrorConstructor() throws InterruptedException, ExecutionException {
        errorsMap.remove(tpOne);
        partitionFutures.complete(errorsMap);
        assertFalse(partitionFutures.isCompletedExceptionally());
        DeleteConsumerGroupOffsetsResult missingPartitionResult =
            new DeleteConsumerGroupOffsetsResult(partitionFutures, partitions);

        TestUtils.assertFutureError(missingPartitionResult.all(), IllegalArgumentException.class);
        assertNull(missingPartitionResult.partitionResult(tpZero).get());
        TestUtils.assertFutureError(missingPartitionResult.partitionResult(tpOne), IllegalArgumentException.class);
    }

    @Test
    public void testPartitionMissingInRequestErrorConstructor() throws InterruptedException, ExecutionException {
        DeleteConsumerGroupOffsetsResult partitionLevelErrorResult = createAndVerifyPartitionLevelErrror();
        assertThrows(IllegalArgumentException.class, () -> partitionLevelErrorResult.partitionResult(new TopicPartition("invalid-topic", 0)));
    }

    @Test
    public void testNoErrorConstructor() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Errors> errorsMap = new HashMap<>();
        errorsMap.put(tpZero, Errors.NONE);
        errorsMap.put(tpOne, Errors.NONE);
        DeleteConsumerGroupOffsetsResult noErrorResult =
            new DeleteConsumerGroupOffsetsResult(partitionFutures, partitions);
        partitionFutures.complete(errorsMap);

        assertNull(noErrorResult.all().get());
        assertNull(noErrorResult.partitionResult(tpZero).get());
        assertNull(noErrorResult.partitionResult(tpOne).get());
    }

    private DeleteConsumerGroupOffsetsResult createAndVerifyPartitionLevelErrror() throws InterruptedException, ExecutionException {
        partitionFutures.complete(errorsMap);
        assertFalse(partitionFutures.isCompletedExceptionally());
        DeleteConsumerGroupOffsetsResult partitionLevelErrorResult =
            new DeleteConsumerGroupOffsetsResult(partitionFutures, partitions);

        TestUtils.assertFutureError(partitionLevelErrorResult.all(), UnknownTopicOrPartitionException.class);
        assertNull(partitionLevelErrorResult.partitionResult(tpZero).get());
        TestUtils.assertFutureError(partitionLevelErrorResult.partitionResult(tpOne), UnknownTopicOrPartitionException.class);
        return partitionLevelErrorResult;
    }
}
