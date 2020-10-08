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
package org.apache.kafka.connect.mirror;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LegacyReplicationPolicyTest {
    @Test
    public void testFormatRemoteTopic() {
        final LegacyReplicationPolicy legacyReplicationPolicy = new LegacyReplicationPolicy();
        assertEquals("aaa", legacyReplicationPolicy.formatRemoteTopic("source1", "aaa"));
        assertEquals("source1.heartbeats", legacyReplicationPolicy.formatRemoteTopic("source1", "heartbeats"));
        assertEquals("source2.source1.heartbeats", legacyReplicationPolicy.formatRemoteTopic("source2", "source1.heartbeats"));

        legacyReplicationPolicy.configure(Collections.singletonMap("replication.policy.separator", "__"));
        assertEquals("aaa", legacyReplicationPolicy.formatRemoteTopic("source1", "aaa"));
        assertEquals("source1__heartbeats", legacyReplicationPolicy.formatRemoteTopic("source1", "heartbeats"));
    }

    @Test
    public void testTopicSource() {
        final LegacyReplicationPolicy legacyReplicationPolicy = new LegacyReplicationPolicy();
        assertNull(legacyReplicationPolicy.topicSource("source1.aaa"));
        assertNull(legacyReplicationPolicy.topicSource("heartbeats"));
        assertEquals("source1", legacyReplicationPolicy.topicSource("source1.heartbeats"));
        assertEquals("source2", legacyReplicationPolicy.topicSource("source2.source1.heartbeats"));
    }

    @Test
    public void testUpstreamTopic() {
        final LegacyReplicationPolicy legacyReplicationPolicy = new LegacyReplicationPolicy();
        assertEquals("aaa", legacyReplicationPolicy.upstreamTopic("aaa"));
        assertEquals("source1.aaa", legacyReplicationPolicy.upstreamTopic("source1.aaa"));
        assertEquals("heartbeats", legacyReplicationPolicy.upstreamTopic("source1.heartbeats"));
    }

    @Test
    public void testOriginalTopic() {
        final LegacyReplicationPolicy legacyReplicationPolicy = new LegacyReplicationPolicy();
        assertEquals("aaa", legacyReplicationPolicy.originalTopic("aaa"));
        assertEquals("source1.aaa", legacyReplicationPolicy.originalTopic("source1.aaa"));
        assertEquals("source2.source1.aaa", legacyReplicationPolicy.originalTopic("source2.source1.aaa"));
        assertEquals("heartbeats", legacyReplicationPolicy.originalTopic("heartbeats"));
        assertEquals("heartbeats", legacyReplicationPolicy.originalTopic("source2.source1.heartbeats"));
    }
}
