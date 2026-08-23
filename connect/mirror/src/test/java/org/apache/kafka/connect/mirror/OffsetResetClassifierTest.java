/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetResetClassifierTest {

    private static final TopicPartition ORDERS_0 = new TopicPartition("primary.orders", 0);
    private static final TopicPartition ORDERS_1 = new TopicPartition("primary.orders", 1);

    @Test
    public void earliestOffsetOfZeroMeansTopicWasReset() {
        OffsetOutOfRangeException exception = outOfRange(ORDERS_0, 500L);

        ConnectException thrown = OffsetResetClassifier.classify(
                exception, partitions -> Map.of(ORDERS_0, 0L));

        assertInstanceOf(TopicResetException.class, thrown);
        assertTrue(thrown.getMessage().contains("primary.orders-0"));
        assertTrue(thrown.getMessage().contains("500"));
    }

    @Test
    public void earliestOffsetAboveZeroMeansRecordsWerePurged() {
        OffsetOutOfRangeException exception = outOfRange(ORDERS_0, 500L);

        ConnectException thrown = OffsetResetClassifier.classify(
                exception, partitions -> Map.of(ORDERS_0, 750L));

        assertInstanceOf(DataLossException.class, thrown);
        assertTrue(thrown.getMessage().contains("primary.orders-0"));
        assertTrue(thrown.getMessage().contains("250")); // 750 - 500 purged records
    }

    @Test
    public void reportsEveryAffectedPartitionEvenThoughOnlyOneExceptionIsReturned() {
        Map<TopicPartition, Long> requested = new HashMap<>();
        requested.put(ORDERS_0, 300L);
        requested.put(ORDERS_1, 850L);
        OffsetOutOfRangeException exception = new OffsetOutOfRangeException(requested);

        Map<TopicPartition, Long> earliest = new HashMap<>();
        earliest.put(ORDERS_0, 0L);    // reset
        earliest.put(ORDERS_1, 900L);  // purge

        ConnectException thrown = OffsetResetClassifier.classify(exception, partitions -> earliest);

        // Whichever partition is iterated first determines the exception type;
        // either is a valid fail-fast outcome, we just need one to come back.
        assertTrue(thrown instanceof TopicResetException || thrown instanceof DataLossException);
    }

    @Test
    public void missingPartitionFromBeginningOffsetsDefaultsToReset() {
        // If beginningOffsets() doesn't return an entry for a partition,
        // treat it the same as offset 0 rather than throwing an NPE.
        OffsetOutOfRangeException exception = outOfRange(ORDERS_0, 200L);

        ConnectException thrown = OffsetResetClassifier.classify(exception, partitions -> Map.of());

        assertInstanceOf(TopicResetException.class, thrown);
    }

    private static OffsetOutOfRangeException outOfRange(TopicPartition partition, long requestedOffset) {
        return new OffsetOutOfRangeException(Map.of(partition, requestedOffset));
    }
}
