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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DataLossException;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataLossDetectorTest {

    private DataLossDetector dataLossDetector;
    private TopicPartition topicPartition;

    @BeforeEach
    public void setUp() {
        LogContext logContext = new LogContext();
        dataLossDetector = new DataLossDetector(logContext);
        topicPartition = new TopicPartition("test-topic", 0);
    }

    @Test
    public void testNoDataLossWithConsecutiveOffsets() {
        // Should not throw exception for consecutive offsets
        assertDoesNotThrow(() -> {
            dataLossDetector.checkForDataLoss(topicPartition, 10L, 11L, 0L, 20L);
        });
    }

    @Test
    public void testDataLossDetectionWithOffsetGap() {
        // Should throw exception for offset gap > 1
        DataLossException exception = assertThrows(DataLossException.class, () -> {
            dataLossDetector.checkForDataLoss(topicPartition, 10L, 15L, 0L, 20L);
        });
        
        assertEquals(DataLossException.DataLossType.OFFSET_GAP, exception.lossType());
        assertEquals(Set.of(topicPartition), exception.partitions());
    }

    @Test
    public void testDataLossDetectionWithOffsetOutOfRange() {
        // Should throw exception when old offset is before beginning offset
        DataLossException exception = assertThrows(DataLossException.class, () -> {
            dataLossDetector.checkForDataLoss(topicPartition, 5L, 10L, 8L, 20L);
        });
        
        assertEquals(DataLossException.DataLossType.OUT_OF_RANGE, exception.lossType());
        assertEquals(Set.of(topicPartition), exception.partitions());
    }

    @Test
    public void testDataLossDetectionWithOffsetBeyondEnd() {
        // Should throw exception when old offset is beyond end offset
        DataLossException exception = assertThrows(DataLossException.class, () -> {
            dataLossDetector.checkForDataLoss(topicPartition, 25L, 10L, 0L, 20L);
        });
        
        assertEquals(DataLossException.DataLossType.OUT_OF_RANGE, exception.lossType());
        assertEquals(Set.of(topicPartition), exception.partitions());
    }

    @Test
    public void testTopicRecreationDetection() {
        // First call to establish baseline
        dataLossDetector.checkForDataLoss(topicPartition, null, 10L, 5L, 20L);
        
        // Second call with higher beginning offset (indicates topic recreation)
        DataLossException exception = assertThrows(DataLossException.class, () -> {
            dataLossDetector.checkForDataLoss(topicPartition, 10L, 15L, 10L, 25L);
        });
        
        assertEquals(DataLossException.DataLossType.TOPIC_RECREATION, exception.lossType());
        assertEquals(Set.of(topicPartition), exception.partitions());
    }

    @Test
    public void testClearPartition() {
        // Establish tracking for partition
        dataLossDetector.checkForDataLoss(topicPartition, null, 10L, 0L, 20L);
        assertEquals(Long.valueOf(10L), dataLossDetector.getLastSeenOffset(topicPartition));
        
        // Clear partition tracking
        dataLossDetector.clearPartition(topicPartition);
        assertEquals(null, dataLossDetector.getLastSeenOffset(topicPartition));
    }

    @Test
    public void testValidateCommittedOffsetSuccess() {
        // Should not throw for valid committed offset
        assertDoesNotThrow(() -> {
            dataLossDetector.validateCommittedOffset(topicPartition, 
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(10L), 5L, 20L);
        });
    }

    @Test
    public void testValidateCommittedOffsetOutOfRange() {
        // Should throw for committed offset out of range
        DataLossException exception = assertThrows(DataLossException.class, () -> {
            dataLossDetector.validateCommittedOffset(topicPartition, 
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(3L), 5L, 20L);
        });
        
        assertEquals(DataLossException.DataLossType.OUT_OF_RANGE, exception.lossType());
        assertEquals(Set.of(topicPartition), exception.partitions());
    }

    @Test
    public void testValidateCommittedOffsetNull() {
        // Should not throw for null committed offset
        assertDoesNotThrow(() -> {
            dataLossDetector.validateCommittedOffset(topicPartition, null, 5L, 20L);
        });
    }
}