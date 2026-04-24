/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.kafka.connect.mirror.tracking;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PartitionOffsetTracker}.
 *
 * This class has no Kafka dependency so these tests are pure unit tests with no mocking needed.
 */
@DisplayName("PartitionOffsetTracker")
class PartitionOffsetTrackerTest {

    private static final TopicPartition TP_0 = new TopicPartition("commit-log", 0);
    private static final TopicPartition TP_1 = new TopicPartition("commit-log", 1);

    private PartitionOffsetTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new PartitionOffsetTracker();
    }

    // =========================================================================

    @Test
    @DisplayName("isTracked returns false for unseen partition")
    void notTrackedInitially() {
        assertFalse(tracker.isTracked(TP_0));
    }

    @Test
    @DisplayName("getNextExpected returns null for unseen partition")
    void getNextExpectedReturnsNullForUnknown() {
        assertNull(tracker.getNextExpected(TP_0));
    }

    @Test
    @DisplayName("recordConsumed stores offset+1 as next expected")
    void recordConsumedStoresOffsetPlusOne() {
        tracker.recordConsumed(TP_0, 42L);
        assertEquals(43L, tracker.getNextExpected(TP_0));
    }

    @Test
    @DisplayName("recordConsumed marks partition as tracked")
    void recordConsumedMarksTracked() {
        tracker.recordConsumed(TP_0, 0L);
        assertTrue(tracker.isTracked(TP_0));
    }

    @Test
    @DisplayName("setNextExpected stores the value directly (no +1)")
    void setNextExpectedStoresDirectly() {
        tracker.setNextExpected(TP_0, 100L);
        assertEquals(100L, tracker.getNextExpected(TP_0));
    }

    @Test
    @DisplayName("setNextExpected marks partition as tracked")
    void setNextExpectedMarksTracked() {
        tracker.setNextExpected(TP_0, 0L);
        assertTrue(tracker.isTracked(TP_0));
    }

    @Test
    @DisplayName("recordConsumed overwrites previous value")
    void recordConsumedOverwrites() {
        tracker.recordConsumed(TP_0, 10L);  // stores 11
        tracker.recordConsumed(TP_0, 50L);  // stores 51
        assertEquals(51L, tracker.getNextExpected(TP_0));
    }

    @Test
    @DisplayName("setNextExpected to 0 on reset clears old value")
    void setNextExpectedToZeroOnReset() {
        tracker.recordConsumed(TP_0, 999L);
        tracker.setNextExpected(TP_0, 0L);
        assertEquals(0L, tracker.getNextExpected(TP_0));
    }

    @Test
    @DisplayName("tracks multiple partitions independently")
    void tracksMultiplePartitionsIndependently() {
        tracker.recordConsumed(TP_0, 100L);
        tracker.recordConsumed(TP_1, 200L);

        assertEquals(101L, tracker.getNextExpected(TP_0));
        assertEquals(201L, tracker.getNextExpected(TP_1));
    }

    @Test
    @DisplayName("snapshot returns immutable copy of state")
    void snapshotIsImmutable() {
        tracker.recordConsumed(TP_0, 5L);
        var snapshot = tracker.snapshot();

        assertThrows(UnsupportedOperationException.class,
            () -> snapshot.put(TP_1, 99L));
    }
}
