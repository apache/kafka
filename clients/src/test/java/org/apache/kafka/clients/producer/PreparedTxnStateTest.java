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
package org.apache.kafka.clients.producer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the PreparedTxnState class part of the #KafkaProducer class.
 */
public class PreparedTxnStateTest {

    @Test
    public void testDefaultConstructor() {
        PreparedTxnState state = new PreparedTxnState();
        assertEquals("", state.toString(), "Empty state should serialize to an empty string");
        assertEquals(-1L, state.producerId(), "Default producerId should be -1");
        assertEquals((short) -1, state.epoch(), "Default epoch should be -1");
        assertFalse(state.hasTransaction(), "Default state should not have a transaction");
    }

    @Test
    public void testParameterizedConstructor() {
        long producerId = 123L;
        short epoch = 45;
        PreparedTxnState state = new PreparedTxnState(producerId, epoch);
        assertEquals(producerId, state.producerId(), "ProducerId should match");
        assertEquals(epoch, state.epoch(), "Epoch should match");
        assertTrue(state.hasTransaction(), "State should have a transaction");
        assertEquals("123:45", state.toString(), "Serialized form should match expected format");
    }

    @Test
    public void testDeserializationFromString() {
        String serialized = "123:45";
        PreparedTxnState state = new PreparedTxnState(serialized);
        assertEquals(serialized, state.toString(), "Deserialized state should match the original serialized string");
        assertEquals(123L, state.producerId(), "Deserialized producerId should match");
        assertEquals((short) 45, state.epoch(), "Deserialized epoch should match");
        assertTrue(state.hasTransaction(), "Deserialized state should have a transaction");
    }

    @Test
    public void testRoundTripSerialization() {
        // Create initialized state from string, then convert back to string
        String original = "9876:54";
        PreparedTxnState state = new PreparedTxnState(original);
        String serialized = state.toString();
        assertEquals(original, serialized, "Round-trip serialization should preserve values");

        // Deserialize again to verify
        PreparedTxnState stateAgain = new PreparedTxnState(serialized);
        assertEquals(original, stateAgain.toString(), "Re-deserialized state should match original");
        assertEquals(state.producerId(), stateAgain.producerId(), "Producer IDs should match");
        assertEquals(state.epoch(), stateAgain.epoch(), "Epochs should match");

        // Test round trip for uninitialized state (empty string)
        String emptyString = "";
        PreparedTxnState emptyState = new PreparedTxnState(emptyString);
        String emptyStateSerialized = emptyState.toString();
        assertEquals(emptyString, emptyStateSerialized, "Round-trip of empty string should remain empty");
        assertEquals(-1L, emptyState.producerId(), "Empty string should result in producerId -1");
        assertEquals((short) -1, emptyState.epoch(), "Empty string should result in epoch -1");

        // Deserialize empty state again to verify
        PreparedTxnState emptyStateAgain = new PreparedTxnState(emptyStateSerialized);
        assertEquals(emptyString, emptyStateAgain.toString(), "Re-deserialized empty state should still be empty");
        assertEquals(-1L, emptyStateAgain.producerId(), "Empty string should result in producerId -1");
        assertEquals((short) -1, emptyStateAgain.epoch(), "Empty string should result in epoch -1");
    }

    @Test
    public void testHandlingOfNullOrEmptyString() {
        PreparedTxnState stateWithNull = new PreparedTxnState(null);
        assertEquals("", stateWithNull.toString(), "Null string should result in empty state");
        assertFalse(stateWithNull.hasTransaction(), "State from null string should not have a transaction");

        PreparedTxnState stateWithEmpty = new PreparedTxnState("");
        assertEquals("", stateWithEmpty.toString(), "Empty string should result in empty state");
        assertFalse(stateWithEmpty.hasTransaction(), "State from empty string should not have a transaction");
    }

    @Test
    public void testMaxValues() {
        // Test with maximum possible values for producer ID and epoch
        String maxValues = Long.MAX_VALUE + ":" + Short.MAX_VALUE;
        PreparedTxnState state = new PreparedTxnState(maxValues);
        assertEquals(maxValues, state.toString(), "Max values should be handled correctly");
        assertEquals(Long.MAX_VALUE, state.producerId(), "Max producer ID should be handled correctly");
        assertEquals(Short.MAX_VALUE, state.epoch(), "Max epoch should be handled correctly");
        assertTrue(state.hasTransaction(), "State with max values should have a transaction");
    }

    @Test
    public void testEqualsAndHashCode() {
        PreparedTxnState state1 = new PreparedTxnState(123L, (short) 45);
        PreparedTxnState state2 = new PreparedTxnState(123L, (short) 45);
        PreparedTxnState state3 = new PreparedTxnState(456L, (short) 78);
        PreparedTxnState state4 = new PreparedTxnState(123L, (short) 46);

        // Test equals
        assertEquals(state1, state2, "Equal states should be equal");
        assertNotEquals(state1, state3, "States with different producer IDs should not be equal");
        assertNotEquals(state1, state4, "States with different epochs should not be equal");
        assertNotEquals(null, state1, "State should not equal null");

        // Test hashCode
        assertEquals(state1.hashCode(), state2.hashCode(), "Equal states should have same hash code");
        assertNotEquals(state1.hashCode(), state3.hashCode(), "Different states should have different hash codes");
    }

    @Test
    public void testHasTransaction() {
        // State with transaction (producer ID >= 0)
        PreparedTxnState stateWithTransaction = new PreparedTxnState(0L, (short) 0);
        assertTrue(stateWithTransaction.hasTransaction(), "State with producerId 0 should have a transaction");

        // State without transaction (producer ID = -1)
        PreparedTxnState stateWithoutTransaction = new PreparedTxnState(-1L, (short) -1);
        assertFalse(stateWithoutTransaction.hasTransaction(), "State with producerId -1 should not have a transaction");
    }

    @Test
    public void testInvalidFormatThrowsException() {
        // Test with invalid format - missing epoch
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("123"),
            "String with missing epoch should throw IllegalArgumentException");

        // Test with invalid format - too many parts
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("123:45:67"),
            "String with extra parts should throw IllegalArgumentException");

        // Test with non-numeric producer ID
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("abc:45"),
            "Non-numeric producer ID should throw IllegalArgumentException");

        // Test with non-numeric epoch
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("123:xyz"),
            "Non-numeric epoch should throw IllegalArgumentException");
    }

    @Test
    public void testInvalidProducerIdEpochCombinations() {
        // Valid combinations: both >= 0
        new PreparedTxnState("0:0");
        new PreparedTxnState("123:45");

        // Invalid: producerId >= 0, epoch < 0
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("123:-2"),
            "Positive producerId with negative epoch (not -1) should throw IllegalArgumentException");

        // Invalid: producerId < 0 (not -1), epoch >= 0
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("-2:45"),
            "Negative producerId (not -1) with positive epoch should throw IllegalArgumentException");

        // Invalid: producerId < 0 (not -1), epoch < 0 (not -1)
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("-2:-2"),
            "Negative producerId and epoch (not -1) should throw IllegalArgumentException");

        // Invalid: producerId = -1, epoch >= 0
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("-1:45"),
            "ProducerId -1 with positive epoch should throw IllegalArgumentException");

        // Invalid: producerId >= 0, epoch = -1
        assertThrows(IllegalArgumentException.class,
            () -> new PreparedTxnState("123:-1"),
            "Positive producerId with epoch -1 should throw IllegalArgumentException");
    }
}
