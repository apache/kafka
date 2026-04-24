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
 
@DisplayName("TopicResetDetector")
class TopicResetDetectorTest {
 
    private static final String TOPIC     = "commit-log";
    private static final int    PARTITION = 0;
 
    private TestConsumerStub   consumer;
    private TopicResetDetector detector;
    private TopicPartition     tp;
 
    @BeforeEach
    void setUp() {
        tp       = new TopicPartition(TOPIC, PARTITION);
        consumer = new TestConsumerStub();
        detector = new TopicResetDetector(consumer);
    }
 
    private void givenEndOffset(long offset) {
        consumer.withEndOffsets(tp, offset);
    }
 
    @Nested
    @DisplayName("when topic has NOT been reset")
    class NoReset {
 
        @Test
        @DisplayName("returns false when consumer position is within topic range")
        void positionWithinRange() {
            givenEndOffset(1000L);
            assertFalse(detector.checkAndRecover(tp, 500L));
        }
 
        @Test
        @DisplayName("returns false when consumer position equals end offset (fully caught up)")
        void positionEqualsEndOffset() {
            givenEndOffset(1000L);
            assertFalse(detector.checkAndRecover(tp, 1000L));
        }
 
        @Test
        @DisplayName("does NOT seek when no reset is detected")
        void doesNotSeekOnNormalOperation() {
            givenEndOffset(1000L);
            detector.checkAndRecover(tp, 500L);
            assertEquals(0, consumer.getSeekCallCount(), "seek() should not be called");
        }
    }
 
    @Nested
    @DisplayName("when topic HAS been reset (deleted and recreated)")
    class ResetDetected {
 
        @Test
        @DisplayName("returns true when consumer position exceeds end offset")
        void detectsReset() {
            givenEndOffset(50L);
            assertTrue(detector.checkAndRecover(tp, 1000L));
        }
 
        @Test
        @DisplayName("seeks consumer to offset 0 on detection")
        void seeksToZero() {
            givenEndOffset(0L);
            detector.checkAndRecover(tp, 500L);
            assertTrue(consumer.wasSeekCalled(tp, 0L), "seek(tp, 0) should have been called");
        }
 
        @Test
        @DisplayName("seeks exactly once even with large position gap")
        void seeksExactlyOnce() {
            givenEndOffset(10L);
            detector.checkAndRecover(tp, 99_999L);
            assertEquals(1, consumer.getSeekCallCount(), "seek() should be called exactly once");
            assertTrue(consumer.wasSeekCalled(tp, 0L));
        }
 
        @Test
        @DisplayName("position of 1 with end offset of 0 is treated as reset")
        void positionOneEndZero() {
            givenEndOffset(0L);
            assertTrue(detector.checkAndRecover(tp, 1L));
        }
    }
 
    @Nested
    @DisplayName("edge cases")
    class EdgeCases {
 
        @Test
        @DisplayName("returns false and does not seek when broker returns null end offset")
        void nullEndOffsetIsSkipped() {
            // no stub → empty map returned → treated as null → skip
            assertFalse(detector.checkAndRecover(tp, 500L));
            assertEquals(0, consumer.getSeekCallCount(), "seek() should not be called");
        }
    }
}