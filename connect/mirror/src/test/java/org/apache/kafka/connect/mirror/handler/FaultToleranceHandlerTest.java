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
package org.apache.kafka.connect.mirror.handler;
 
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.test.TestConsumerStub;
import org.apache.kafka.connect.mirror.tracking.PartitionOffsetTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
 
import static org.junit.jupiter.api.Assertions.*;
 
@DisplayName("FaultToleranceHandler")
class FaultToleranceHandlerTest {
 
    private static final String TOPIC     = "commit-log";
    private static final int    PARTITION = 0;
 
    private TestConsumerStub       consumer;
    private PartitionOffsetTracker tracker;
    private FaultToleranceHandler  handler;
    private TopicPartition         tp;
 
    @BeforeEach
    void setUp() {
        tp       = new TopicPartition(TOPIC, PARTITION);
        consumer = new TestConsumerStub();
        tracker  = new PartitionOffsetTracker();
        handler  = new FaultToleranceHandler(consumer, tracker);
    }
 
    @Nested
    @DisplayName("first-run behaviour (partition not yet tracked)")
    class FirstRun {
 
        @Test
        @DisplayName("skips all checks if partition has never been tracked")
        void skipsChecksIfNotTracked() {
            assertDoesNotThrow(() -> handler.runChecks(tp, consumer));
            assertEquals(0, consumer.getSeekCallCount(), "seek() should never be called");
        }
    }
 
    @Nested
    @DisplayName("normal operation (no faults)")
    class NormalOperation {
 
        @BeforeEach
        void seedTracker() {
            tracker.setNextExpected(tp, 100L);
        }
 
        @Test
        @DisplayName("passes without exception when topic is healthy")
        void noExceptionOnHealthyTopic() {
            consumer.withBeginningOffsets(tp, 10L);  // no truncation
            consumer.withPosition(tp, 100L);
            consumer.withEndOffsets(tp, 200L);        // no reset
 
            assertDoesNotThrow(() -> handler.runChecks(tp, consumer));
        }
 
        @Test
        @DisplayName("does not seek when no reset is detected")
        void noSeekOnNormalOperation() {
            consumer.withBeginningOffsets(tp, 50L);
            consumer.withPosition(tp, 100L);
            consumer.withEndOffsets(tp, 300L);
 
            handler.runChecks(tp, consumer);
            assertEquals(0, consumer.getSeekCallCount(), "seek() should not be called");
        }
    }
 
    @Nested
    @DisplayName("truncation detection (Task 2)")
    class TruncationScenario {
 
        @Test
        @DisplayName("propagates RuntimeException from TruncationDetector")
        void propagatesTruncationException() {
            tracker.setNextExpected(tp, 100L);
            consumer.withBeginningOffsets(tp, 500L);  // gap → truncation
 
            assertThrows(RuntimeException.class, () -> handler.runChecks(tp, consumer));
        }
    }
 
    @Nested
    @DisplayName("topic reset recovery (Task 3)")
    class ResetScenario {
 
        @Test
        @DisplayName("seeks to 0 and resets tracker on topic reset")
        void seeksAndResetsTrackerOnReset() {
            tracker.setNextExpected(tp, 1000L);
 
            consumer.withBeginningOffsets(tp, 0L);   // no truncation
            consumer.withPosition(tp, 1000L);
            consumer.withEndOffsets(tp, 50L);         // reset: position > endOffset
 
            handler.runChecks(tp, consumer);
 
            assertTrue(consumer.wasSeekCalled(tp, 0L), "seek(tp, 0) should have been called");
            assertEquals(0L, tracker.getNextExpected(tp),
                "Tracker should be reset to 0 after topic reset recovery");
        }
    }
}