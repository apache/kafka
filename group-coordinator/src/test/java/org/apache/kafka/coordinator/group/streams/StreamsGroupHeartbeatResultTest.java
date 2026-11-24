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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StreamsGroupHeartbeatResultTest {

    @Test
    public void testConstructorPreservesEpochs() {
        StreamsGroupHeartbeatResult result = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 7, 5, 3);
        assertEquals(7, result.currentTopologyEpoch());
        assertEquals(5, result.storedDescriptionTopologyEpoch());
        assertEquals(3, result.failedDescriptionTopologyEpoch());
    }

    @Test
    public void testCurrentTopologyEpochIsPartOfEquality() {
        StreamsGroupHeartbeatResult a = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 1, -1, -1);
        StreamsGroupHeartbeatResult b = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 2, -1, -1);
        assertNotEquals(a, b);

        StreamsGroupHeartbeatResult c = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 1, -1, -1);
        assertEquals(a, c);
    }

    @Test
    public void testStoredAndFailedEpochsArePartOfEquality() {
        StreamsGroupHeartbeatResult a = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 1, 1, -1);
        StreamsGroupHeartbeatResult differentStored = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 1, 0, -1);
        StreamsGroupHeartbeatResult differentFailed = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), 1, 1, 0);
        assertNotEquals(a, differentStored);
        assertNotEquals(a, differentFailed);
    }

    @Test
    public void testCreatableTopicsMapIsImmutable() {
        StreamsGroupHeartbeatResult result = new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(), Map.of(), -1, -1, -1);
        assertThrows(UnsupportedOperationException.class,
            () -> result.creatableTopics().put("t", null));
    }

    @Test
    public void testNullDataIsRejected() {
        assertThrows(NullPointerException.class,
            () -> new StreamsGroupHeartbeatResult(null, Map.of(), -1, -1, -1));
    }
}
