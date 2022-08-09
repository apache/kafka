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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class LogReplayTrackerTest {
    @Test
    public void testEmpty() {
        LogReplayTracker tracker = new LogReplayTracker.Builder().build();
        assertTrue(tracker.empty());
        tracker.replay(new NoOpRecord());
        assertFalse(tracker.empty());
    }

    @Test
    public void testVersionless() {
        LogReplayTracker tracker = new LogReplayTracker.Builder().build();
        assertTrue(tracker.versionless());
        tracker.replay(new NoOpRecord());
        assertTrue(tracker.versionless());
        tracker.replay(new FeatureLevelRecord().
                setName("foo.bar").
                setFeatureLevel((short) 1));
        assertTrue(tracker.versionless());
        tracker.replay(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel((short) 2));
        assertFalse(tracker.versionless());
    }

    @Test
    public void testNewBytesSinceLastSnapshot() {
        LogReplayTracker tracker = new LogReplayTracker.Builder().build();
        assertEquals(0L, tracker.newBytesSinceLastSnapshot());
        tracker.addNewBytesSinceLastSnapshot(100);
        assertEquals(100L, tracker.newBytesSinceLastSnapshot());
        tracker.resetNewBytesSinceLastSnapshot();
        assertEquals(0L, tracker.newBytesSinceLastSnapshot());
        tracker.addNewBytesSinceLastSnapshot(1000);
        assertEquals(1000L, tracker.newBytesSinceLastSnapshot());
        tracker.addNewBytesSinceLastSnapshot(1000);
        assertEquals(2000L, tracker.newBytesSinceLastSnapshot());
        tracker.resetToEmpty();
        assertEquals(0L, tracker.newBytesSinceLastSnapshot());

    }
}
