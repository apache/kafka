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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AcknowledgementCommitCallbackHandlerTest {

    private AcknowledgementCommitCallbackHandler acknowledgementCommitCallbackHandler;
    private Map<TopicPartitionAndOffset, Exception> exceptionMap;
    private final TopicPartition tp0 = new TopicPartition("test-topic", 0);
    private final TopicIdPartition tip0 = new TopicIdPartition(Uuid.randomUuid(), tp0);
    private final TopicPartitionAndOffset tpo00 = new TopicPartitionAndOffset(tip0, 0L);
    private final TopicPartitionAndOffset tpo01 = new TopicPartitionAndOffset(tip0, 1L);
    private final TopicPartition tp1 = new TopicPartition("test-topic-2", 0);
    private final TopicIdPartition tip1 = new TopicIdPartition(Uuid.randomUuid(), tp1);
    private final TopicPartitionAndOffset tpo10 = new TopicPartitionAndOffset(tip1, 0L);
    private final TopicPartition tp2 = new TopicPartition("test-topic-2", 1);
    private final TopicIdPartition tip2 = new TopicIdPartition(Uuid.randomUuid(), tp2);
    private final TopicPartitionAndOffset tpo20 = new TopicPartitionAndOffset(tip2, 0L);
    private Map<TopicIdPartition, Acknowledgements> acknowledgementsMap;

    @BeforeEach
    public void setup() {
        acknowledgementsMap = new HashMap<>();
        exceptionMap = new LinkedHashMap<>();
        TestableAcknowledgeCommitCallBack callback = new TestableAcknowledgeCommitCallBack();
        acknowledgementCommitCallbackHandler = new AcknowledgementCommitCallbackHandler(callback);
    }

    @Test
    public void testNoException() {
        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements.add(1L, AcknowledgeType.REJECT);
        acknowledgementsMap.put(tip0, acknowledgements);

        acknowledgementCommitCallbackHandler.onComplete(acknowledgementsMap);

        assertNull(exceptionMap.get(tpo00));
        assertNull(exceptionMap.get(tpo01));
    }

    @Test
    public void testInvalidRecord() {
        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements.add(1L, AcknowledgeType.REJECT);
        acknowledgements.setAcknowledgeErrorCode(Errors.INVALID_RECORD_STATE);
        acknowledgementsMap.put(tip0, acknowledgements);

        acknowledgementCommitCallbackHandler.onComplete(acknowledgementsMap);
        assertTrue(exceptionMap.get(tpo00) instanceof InvalidRecordStateException);
        assertTrue(exceptionMap.get(tpo01) instanceof InvalidRecordStateException);
    }

    @Test
    public void testUnauthorizedTopic() {
        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements.add(1L, AcknowledgeType.REJECT);
        acknowledgements.setAcknowledgeErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED);
        acknowledgementsMap.put(tip0, acknowledgements);

        acknowledgementCommitCallbackHandler.onComplete(acknowledgementsMap);
        assertTrue(exceptionMap.get(tpo00) instanceof TopicAuthorizationException);
        assertTrue(exceptionMap.get(tpo01) instanceof TopicAuthorizationException);
    }

    @Test
    public void testMultiplePartitions() {
        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements.add(1L, AcknowledgeType.REJECT);
        acknowledgements.setAcknowledgeErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED);
        acknowledgementsMap.put(tip0, acknowledgements);

        Acknowledgements acknowledgements1 = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.RELEASE);
        acknowledgements1.setAcknowledgeErrorCode(Errors.INVALID_RECORD_STATE);
        acknowledgementsMap.put(tip1, acknowledgements1);

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(0L, AcknowledgeType.ACCEPT);
        acknowledgementsMap.put(tip2, acknowledgements2);

        acknowledgementCommitCallbackHandler.onComplete(acknowledgementsMap);

        assertTrue(exceptionMap.get(tpo00) instanceof TopicAuthorizationException);
        assertTrue(exceptionMap.get(tpo01) instanceof TopicAuthorizationException);
        assertTrue(exceptionMap.get(tpo10) instanceof InvalidRecordStateException);
        assertNull(exceptionMap.get(tpo20));
    }

    private class TestableAcknowledgeCommitCallBack implements AcknowledgementCommitCallback {
        @Override
        public void onComplete(Map<TopicIdPartition, Set<OffsetAndMetadata>> offsetsMap, Exception exception) {
            offsetsMap.forEach((partition, offsetAndMetadata) -> offsetAndMetadata.forEach(offset -> {
                TopicPartitionAndOffset tpo = new TopicPartitionAndOffset(partition, offset.offset());
                exceptionMap.put(tpo, exception);
            }));
        }
    }

    private static class TopicPartitionAndOffset {
        TopicIdPartition topicIdPartition;
        Long offset;
        TopicPartitionAndOffset(TopicIdPartition topicIdPartition, Long offset) {
            this.topicIdPartition = topicIdPartition;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicPartitionAndOffset that = (TopicPartitionAndOffset) o;
            return Objects.equals(topicIdPartition, that.topicIdPartition) &&
                 Objects.equals(offset, that.offset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, offset);
        }
    }
}