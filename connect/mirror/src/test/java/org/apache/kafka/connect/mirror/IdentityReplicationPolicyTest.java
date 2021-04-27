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

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class IdentityReplicationPolicyTest {
    @Test
    public void testFormatRemoteTopic() {
        final IdentityReplicationPolicy identityReplicationPolicy = new IdentityReplicationPolicy();
        assertEquals("aaa", identityReplicationPolicy.formatRemoteTopic("source1", "aaa"));
        assertEquals("source1.heartbeats", identityReplicationPolicy.formatRemoteTopic("source1", "heartbeats"));
        assertEquals("source2.source1.heartbeats", identityReplicationPolicy.formatRemoteTopic("source2", "source1.heartbeats"));

        identityReplicationPolicy.configure(Collections.singletonMap("replication.policy.separator", "__"));
        assertEquals("aaa", identityReplicationPolicy.formatRemoteTopic("source1", "aaa"));
        assertEquals("source1__heartbeats", identityReplicationPolicy.formatRemoteTopic("source1", "heartbeats"));
    }

    @Test
    public void testTopicSource() {
        final IdentityReplicationPolicy identityReplicationPolicy = new IdentityReplicationPolicy();
        assertNull(identityReplicationPolicy.topicSource("source1.aaa"));
        assertNull(identityReplicationPolicy.topicSource("heartbeats"));
        assertEquals("source1", identityReplicationPolicy.topicSource("source1.heartbeats"));
        assertEquals("source2", identityReplicationPolicy.topicSource("source2.source1.heartbeats"));
    }

    @Test
    public void testUpstreamTopic() {
        final IdentityReplicationPolicy identityReplicationPolicy = new IdentityReplicationPolicy();
        assertEquals("aaa", identityReplicationPolicy.upstreamTopic("aaa"));
        assertEquals("source1.aaa", identityReplicationPolicy.upstreamTopic("source1.aaa"));
        assertEquals("heartbeats", identityReplicationPolicy.upstreamTopic("source1.heartbeats"));
    }

    @Test
    public void testOriginalTopic() {
        final IdentityReplicationPolicy identityReplicationPolicy = new IdentityReplicationPolicy();
        assertEquals("aaa", identityReplicationPolicy.originalTopic("aaa"));
        assertEquals("source1.aaa", identityReplicationPolicy.originalTopic("source1.aaa"));
        assertEquals("source2.source1.aaa", identityReplicationPolicy.originalTopic("source2.source1.aaa"));
        assertEquals("heartbeats", identityReplicationPolicy.originalTopic("heartbeats"));
        assertEquals("heartbeats", identityReplicationPolicy.originalTopic("source2.source1.heartbeats"));
    }
}
