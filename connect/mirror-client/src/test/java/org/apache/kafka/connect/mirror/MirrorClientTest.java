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

import org.apache.kafka.common.Configurable;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MirrorClientTest {

    private static class FakeMirrorClient extends MirrorClient {

        List<String> topics;

        FakeMirrorClient(List<String> topics) {
            this(new DefaultReplicationPolicy(), topics);
        }

        FakeMirrorClient(ReplicationPolicy replicationPolicy, List<String> topics) {
            super(null, replicationPolicy, null);
            this.topics = topics;
        }

        FakeMirrorClient() {
            this(Collections.emptyList());
        } 

        @Override
        protected Set<String> listTopics() {
            return new HashSet<>(topics);
        }
    }

    @Test
    public void testIsHeartbeatTopic() {
        MirrorClient client = new FakeMirrorClient();
        assertTrue(client.isHeartbeatTopic("heartbeats"));
        assertTrue(client.isHeartbeatTopic("source1.heartbeats"));
        assertTrue(client.isHeartbeatTopic("source2.source1.heartbeats"));
        assertFalse(client.isHeartbeatTopic("heartbeats!"));
        assertFalse(client.isHeartbeatTopic("!heartbeats"));
        assertFalse(client.isHeartbeatTopic("source1heartbeats"));
        assertFalse(client.isHeartbeatTopic("source1-heartbeats"));
    }

    @Test
    public void testIsCheckpointTopic() {
        MirrorClient client = new FakeMirrorClient();
        assertTrue(client.isCheckpointTopic("source1.checkpoints.internal"));
        assertFalse(client.isCheckpointTopic("checkpoints.internal"));
        assertFalse(client.isCheckpointTopic("checkpoints-internal"));
        assertFalse(client.isCheckpointTopic("checkpoints.internal!"));
        assertFalse(client.isCheckpointTopic("!checkpoints.internal"));
        assertFalse(client.isCheckpointTopic("source1checkpointsinternal"));
    }

    @Test
    public void countHopsForTopicTest() {
        MirrorClient client = new FakeMirrorClient();
        assertEquals(-1, client.countHopsForTopic("topic", "source"));
        assertEquals(-1, client.countHopsForTopic("source", "source"));
        assertEquals(-1, client.countHopsForTopic("sourcetopic", "source"));
        assertEquals(-1, client.countHopsForTopic("source1.topic", "source2"));
        assertEquals(1, client.countHopsForTopic("source1.topic", "source1"));
        assertEquals(1, client.countHopsForTopic("source2.source1.topic", "source2"));
        assertEquals(2, client.countHopsForTopic("source2.source1.topic", "source1"));
        assertEquals(3, client.countHopsForTopic("source3.source2.source1.topic", "source1"));
        assertEquals(-1, client.countHopsForTopic("source3.source2.source1.topic", "source4"));
    }

    @Test
    public void heartbeatTopicsTest() throws InterruptedException {
        MirrorClient client = new FakeMirrorClient(Arrays.asList("topic1", "topic2", "heartbeats",
            "source1.heartbeats", "source2.source1.heartbeats", "source3.heartbeats"));
        Set<String> heartbeatTopics = client.heartbeatTopics();
        assertEquals(heartbeatTopics, new HashSet<>(Arrays.asList("heartbeats", "source1.heartbeats",
            "source2.source1.heartbeats", "source3.heartbeats")));
    }

    @Test
    public void checkpointsTopicsTest() throws InterruptedException {
        MirrorClient client = new FakeMirrorClient(Arrays.asList("topic1", "topic2", "checkpoints.internal",
            "source1.checkpoints.internal", "source2.source1.checkpoints.internal", "source3.checkpoints.internal"));
        Set<String> checkpointTopics = client.checkpointTopics();
        assertEquals(new HashSet<>(Arrays.asList("source1.checkpoints.internal",
            "source2.source1.checkpoints.internal", "source3.checkpoints.internal")), checkpointTopics);
    }

    @Test
    public void replicationHopsTest() throws InterruptedException {
        MirrorClient client = new FakeMirrorClient(Arrays.asList("topic1", "topic2", "heartbeats",
            "source1.heartbeats", "source1.source2.heartbeats", "source3.heartbeats"));
        assertEquals(1, client.replicationHops("source1"));
        assertEquals(2, client.replicationHops("source2")); 
        assertEquals(1, client.replicationHops("source3"));
        assertEquals(-1, client.replicationHops("source4"));
    }

    @Test
    public void upstreamClustersTest() throws InterruptedException {
        MirrorClient client = new FakeMirrorClient(Arrays.asList("topic1", "topic2", "heartbeats",
            "source1.heartbeats", "source1.source2.heartbeats", "source3.source4.source5.heartbeats"));
        Set<String> sources = client.upstreamClusters();
        assertTrue(sources.contains("source1"));
        assertTrue(sources.contains("source2"));
        assertTrue(sources.contains("source3"));
        assertTrue(sources.contains("source4"));
        assertTrue(sources.contains("source5"));
        assertFalse(sources.contains("sourceX"));
        assertFalse(sources.contains(""));
        assertFalse(sources.contains(null));
    }

    @Test
    public void testIdentityReplicationUpstreamClusters() throws InterruptedException {
        // IdentityReplicationPolicy treats heartbeats as a special case, so these should work as usual.
        MirrorClient client = new FakeMirrorClient(identityReplicationPolicy("source"), Arrays.asList("topic1",
            "topic2", "heartbeats", "source1.heartbeats", "source1.source2.heartbeats",
            "source3.source4.source5.heartbeats"));
        Set<String> sources = client.upstreamClusters();
        assertTrue(sources.contains("source1"));
        assertTrue(sources.contains("source2"));
        assertTrue(sources.contains("source3"));
        assertTrue(sources.contains("source4"));
        assertTrue(sources.contains("source5"));
        assertFalse(sources.contains(""));
        assertFalse(sources.contains(null));
        assertEquals(5, sources.size());
    }

    @Test
    public void remoteTopicsTest() throws InterruptedException {
        MirrorClient client = new FakeMirrorClient(Arrays.asList("topic1", "topic2", "topic3",
            "source1.topic4", "source1.source2.topic5", "source3.source4.source5.topic6"));
        Set<String> remoteTopics = client.remoteTopics();
        assertFalse(remoteTopics.contains("topic1"));
        assertFalse(remoteTopics.contains("topic2"));
        assertFalse(remoteTopics.contains("topic3"));
        assertTrue(remoteTopics.contains("source1.topic4"));
        assertTrue(remoteTopics.contains("source1.source2.topic5"));
        assertTrue(remoteTopics.contains("source3.source4.source5.topic6"));
    }

    @Test
    public void testIdentityReplicationRemoteTopics() throws InterruptedException {
        // IdentityReplicationPolicy should consider any topic to be remote.
        MirrorClient client = new FakeMirrorClient(identityReplicationPolicy("source"), Arrays.asList(
            "topic1", "topic2", "topic3", "heartbeats", "backup.heartbeats"));
        Set<String> remoteTopics = client.remoteTopics();
        assertTrue(remoteTopics.contains("topic1"));
        assertTrue(remoteTopics.contains("topic2"));
        assertTrue(remoteTopics.contains("topic3"));
        // Heartbeats are treated as a special case
        assertFalse(remoteTopics.contains("heartbeats"));
        assertTrue(remoteTopics.contains("backup.heartbeats"));
    }

    @Test
    public void remoteTopicsSeparatorTest() throws InterruptedException {
        MirrorClient client = new FakeMirrorClient(Arrays.asList("topic1", "topic2", "topic3",
            "source1__topic4", "source1__source2__topic5", "source3__source4__source5__topic6"));
        ((Configurable) client.replicationPolicy()).configure(
            Collections.singletonMap("replication.policy.separator", "__"));
        Set<String> remoteTopics = client.remoteTopics();
        assertFalse(remoteTopics.contains("topic1"));
        assertFalse(remoteTopics.contains("topic2"));
        assertFalse(remoteTopics.contains("topic3"));
        assertTrue(remoteTopics.contains("source1__topic4"));
        assertTrue(remoteTopics.contains("source1__source2__topic5"));
        assertTrue(remoteTopics.contains("source3__source4__source5__topic6"));
    }

    @Test
    public void testIdentityReplicationTopicSource() {
        MirrorClient client = new FakeMirrorClient(
            identityReplicationPolicy("primary"), Collections.emptyList());
        assertEquals("topic1", client.replicationPolicy()
            .formatRemoteTopic("primary", "topic1"));
        assertEquals("primary", client.replicationPolicy()
            .topicSource("topic1"));
        // Heartbeats are handled as a special case
        assertEquals("backup.heartbeats", client.replicationPolicy()
            .formatRemoteTopic("backup", "heartbeats"));
        assertEquals("backup", client.replicationPolicy()
            .topicSource("backup.heartbeats"));
    }

    private ReplicationPolicy identityReplicationPolicy(String source) {
        IdentityReplicationPolicy policy = new IdentityReplicationPolicy();
        policy.configure(Collections.singletonMap(
            IdentityReplicationPolicy.SOURCE_CLUSTER_ALIAS_CONFIG, source));
        return policy;
    }
}
