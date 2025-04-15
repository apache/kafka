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
        KafkaProducer.PreparedTxnState state = new KafkaProducer.PreparedTxnState();
        assertEquals("-1:-1", state.toString(), "Empty state should serialize to -1:-1");
        assertEquals(-1L, state.producerId(), "Default producerId should be -1");
        assertEquals((short) -1, state.epoch(), "Default epoch should be -1");
        assertFalse(state.isValid(), "Default state should not be valid");
    }
    
    @Test
    public void testParameterizedConstructor() {
        long producerId = 123L;
        short epoch = 45;
        KafkaProducer.PreparedTxnState state = new KafkaProducer.PreparedTxnState(producerId, epoch);
        assertEquals(producerId, state.producerId(), "ProducerId should match");
        assertEquals(epoch, state.epoch(), "Epoch should match");
        assertTrue(state.isValid(), "State should be valid");
        assertEquals("123:45", state.toString(), "Serialized form should match expected format");
    }

    @Test
    public void testDeserializationFromString() {
        String serialized = "123:45";
        KafkaProducer.PreparedTxnState state = new KafkaProducer.PreparedTxnState(serialized);
        assertEquals(serialized, state.toString(), "Deserialized state should match the original serialized string");
        assertEquals(123L, state.producerId(), "Deserialized producerId should match");
        assertEquals((short) 45, state.epoch(), "Deserialized epoch should match");
        assertTrue(state.isValid(), "Deserialized state should be valid");
    }

    @Test
    public void testRoundTripSerialization() {
        // Create from string, then convert back to string
        String original = "9876:54";
        KafkaProducer.PreparedTxnState state = new KafkaProducer.PreparedTxnState(original);
        String serialized = state.toString();
        assertEquals(original, serialized, "Round-trip serialization should preserve values");
        
        // Deserialize again to verify
        KafkaProducer.PreparedTxnState stateAgain = new KafkaProducer.PreparedTxnState(serialized);
        assertEquals(original, stateAgain.toString(), "Re-deserialized state should match original");
        assertEquals(state.producerId(), stateAgain.producerId(), "Producer IDs should match");
        assertEquals(state.epoch(), stateAgain.epoch(), "Epochs should match");
    }

    @Test
    public void testHandlingOfNullOrEmptyString() {
        KafkaProducer.PreparedTxnState stateWithNull = new KafkaProducer.PreparedTxnState(null);
        assertEquals("-1:-1", stateWithNull.toString(), "Null string should result in empty state");
        assertFalse(stateWithNull.isValid(), "State from null string should not be valid");
        
        KafkaProducer.PreparedTxnState stateWithEmpty = new KafkaProducer.PreparedTxnState("");
        assertEquals("-1:-1", stateWithEmpty.toString(), "Empty string should result in empty state");
        assertFalse(stateWithEmpty.isValid(), "State from empty string should not be valid");
    }

    @Test
    public void testMaxValues() {
        // Test with maximum possible values for producer ID and epoch
        String maxValues = Long.MAX_VALUE + ":" + Short.MAX_VALUE;
        KafkaProducer.PreparedTxnState state = new KafkaProducer.PreparedTxnState(maxValues);
        assertEquals(maxValues, state.toString(), "Max values should be handled correctly");
        assertEquals(Long.MAX_VALUE, state.producerId(), "Max producer ID should be handled correctly");
        assertEquals(Short.MAX_VALUE, state.epoch(), "Max epoch should be handled correctly");
        assertTrue(state.isValid(), "State with max values should be valid");
    }

    @Test
    public void testEqualsAndHashCode() {
        KafkaProducer.PreparedTxnState state1 = new KafkaProducer.PreparedTxnState(123L, (short) 45);
        KafkaProducer.PreparedTxnState state2 = new KafkaProducer.PreparedTxnState(123L, (short) 45);
        KafkaProducer.PreparedTxnState state3 = new KafkaProducer.PreparedTxnState(456L, (short) 78);
        KafkaProducer.PreparedTxnState state4 = new KafkaProducer.PreparedTxnState(123L, (short) 46);
        
        // Test equals
        assertEquals(state1, state2, "Equal states should be equal");
        assertNotEquals(state1, state3, "States with different producer IDs should not be equal");
        assertNotEquals(state1, state4, "States with different epochs should not be equal");
        assertNotEquals(state1, null, "State should not equal null");
        assertNotEquals(state1, "not a state", "State should not equal non-state object");
        
        // Test hashCode
        assertEquals(state1.hashCode(), state2.hashCode(), "Equal states should have same hash code");
        assertNotEquals(state1.hashCode(), state3.hashCode(), "Different states should have different hash codes");
    }
    
    @Test
    public void testIsValid() {
        // Valid state (producer ID >= 0)
        KafkaProducer.PreparedTxnState validState = new KafkaProducer.PreparedTxnState(0L, (short) 0);
        assertTrue(validState.isValid(), "State with producerId 0 should be valid");
        
        // Invalid state (producer ID = -1)
        KafkaProducer.PreparedTxnState invalidState = new KafkaProducer.PreparedTxnState(-1L, (short) 0);
        assertFalse(invalidState.isValid(), "State with producerId -1 should not be valid");
    }

    @Test
    public void testInvalidFormatThrowsException() {
        // Test with invalid format - missing epoch
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducer.PreparedTxnState("123"),
                "String with missing epoch should throw IllegalArgumentException");
        
        // Test with invalid format - too many parts
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducer.PreparedTxnState("123:45:67"),
                "String with extra parts should throw IllegalArgumentException");
        
        // Test with non-numeric producer ID
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducer.PreparedTxnState("abc:45"),
                "Non-numeric producer ID should throw IllegalArgumentException");
        
        // Test with non-numeric epoch
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducer.PreparedTxnState("123:xyz"),
                "Non-numeric epoch should throw IllegalArgumentException");
    }
} 