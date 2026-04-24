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
package org.apache.kafka.connect.mirror.detector;
 
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.test.TestConsumerStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
 
import static org.junit.jupiter.api.Assertions.*;
 
@DisplayName("TruncationDetector")
class TruncationDetectorTest {
 
    private static final String TOPIC     = "commit-log";
    private static final int    PARTITION = 0;
 
    private TestConsumerStub   consumer;
    private TruncationDetector detector;
    private TopicPartition     tp;
 
    @BeforeEach
    void setUp() {
        tp       = new TopicPartition(TOPIC, PARTITION);
        consumer = new TestConsumerStub();
        detector = new TruncationDetector(consumer);
    }
 
    private void givenBeginningOffset(long offset) {
        consumer.withBeginningOffsets(tp, offset);
    }
 
    @Nested
    @DisplayName("when no truncation has occurred")
    class NoTruncation {
 
        @Test
        @DisplayName("passes silently when beginningOffset == lastLoadedOffset")
        void exactMatch() {
            givenBeginningOffset(100L);
            assertDoesNotThrow(() -> detector.check(tp, 100L));
        }
 
        @Test
        @DisplayName("passes silently when beginningOffset < lastLoadedOffset (normal read-ahead)")
        void beginningBehindBookmark() {
            givenBeginningOffset(50L);
            assertDoesNotThrow(() -> detector.check(tp, 100L));
        }
 
        @Test
        @DisplayName("passes at offset 0 — fresh topic, nothing consumed yet except first record")
        void freshTopicAtZero() {
            givenBeginningOffset(0L);
            assertDoesNotThrow(() -> detector.check(tp, 0L));
        }
    }
 
    @Nested
    @DisplayName("when log truncation is detected")
    class TruncationDetected {
 
        @Test
        @DisplayName("throws RuntimeException when beginningOffset > lastLoadedOffset")
        void throwsOnGap() {
            givenBeginningOffset(500L);
            RuntimeException ex = assertThrows(
                RuntimeException.class,
                () -> detector.check(tp, 100L)
            );
            assertTrue(ex.getMessage().contains("Log truncation detected"));
        }
 
        @Test
        @DisplayName("exception message includes topic-partition details")
        void exceptionMessageContainsTopicPartition() {
            givenBeginningOffset(200L);
            RuntimeException ex = assertThrows(
                RuntimeException.class,
                () -> detector.check(tp, 50L)
            );
            assertTrue(ex.getMessage().contains(TOPIC));
        }
 
        @Test
        @DisplayName("exception message reports correct number of lost messages")
        void exceptionMessageContainsLostCount() {
            givenBeginningOffset(300L);
            RuntimeException ex = assertThrows(
                RuntimeException.class,
                () -> detector.check(tp, 100L)
            );
            assertTrue(ex.getMessage().contains("200"));
        }
 
        @Test
        @DisplayName("gap of exactly 1 is still treated as truncation")
        void gapOfOneIsThrown() {
            givenBeginningOffset(101L);
            assertThrows(RuntimeException.class, () -> detector.check(tp, 100L));
        }
    }
 
    @Nested
    @DisplayName("edge cases")
    class EdgeCases {
 
        @Test
        @DisplayName("skips check and logs warning when broker returns null beginning offset")
        void nullBeginningOffsetIsSkipped() {
            // no stub → empty map returned → null offset → skip check
            assertDoesNotThrow(() -> detector.check(tp, 100L));
        }
    }
}
 