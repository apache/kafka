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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.replica.ClientMetadata;
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RemoteFetchPrincipalMetricsTest {
    private static final long TTL_MS = 3_600_000L;
    private Metrics metrics;

    @BeforeEach
    public void setUp() {
        metrics = new Metrics();
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    private static Optional<ClientMetadata> client(String user, String clientId) {
        return Optional.of(new DefaultClientMetadata("", clientId, null, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), "PLAINTEXT"));
    }

    private Double total(String name, Map<String, String> tags) {
        KafkaMetric metric = metrics.metric(new MetricName(name, RemoteFetchPrincipalMetrics.GROUP, "", tags));
        return metric == null ? null : (Double) metric.metricValue();
    }

    private long metricCount() {
        return metrics.metrics().keySet().stream().filter(n -> n.group().equals(RemoteFetchPrincipalMetrics.GROUP)).count();
    }

    @Test
    public void testMetricsAreCreatedLazilyAndTaggedByUserOnlyByDefault() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, false, false, TTL_MS);
        assertEquals(0, metricCount());

        m.recordRequest("t1", client("alice", "c1"), 1000);
        m.recordBytes("t1", client("alice", "c1"), 100, 1000);
        m.recordBytes("t2", client("alice", "c2"), 50, 1000);
        m.recordBytes("t1", client("bob", "c1"), 7, 1000);

        Map<String, String> alice = Map.of("user", "alice");
        Map<String, String> bob = Map.of("user", "bob");
        assertEquals(150.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, alice));
        assertEquals(1.0, total(RemoteFetchPrincipalMetrics.REQUEST_TOTAL, alice));
        assertEquals(7.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, bob));
        assertEquals(0.0, total(RemoteFetchPrincipalMetrics.REQUEST_TOTAL, bob));
        assertEquals(2, m.trackedKeys());
        assertEquals(8, metricCount());
    }

    @Test
    public void testTopicTagIsOnlyPresentWhenEnabled() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, true, false, TTL_MS);
        m.recordBytes("t1", client("alice", "c1"), 10, 1000);
        m.recordBytes("t2", client("alice", "c1"), 20, 1000);

        assertEquals(10.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "alice", "topic", "t1")));
        assertEquals(20.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "alice", "topic", "t2")));
        assertNull(total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "alice")));
    }

    @Test
    public void testClientIdTagIsOnlyPresentWhenEnabled() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, true, true, TTL_MS);
        m.recordBytes("t1", client("alice", "c1"), 10, 1000);
        m.recordBytes("t1", client("alice", "c2"), 20, 1000);

        assertEquals(10.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "alice", "topic", "t1", "client-id", "c1")));
        assertEquals(20.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "alice", "topic", "t1", "client-id", "c2")));
        assertNull(total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "alice", "topic", "t1")));
    }

    @Test
    public void testFetchesWithoutUsableIdentityAreNotRecorded() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, true, true, TTL_MS);
        m.recordBytes("t1", Optional.empty(), 10, 1000);
        m.recordBytes("t1", client("alice", ""), 10, 1000);
        m.recordBytes("t1", client("alice", " "), 10, 1000);
        m.recordBytes("t1", client("alice", null), 10, 1000);
        m.recordBytes("t1", client(" ", "c1"), 10, 1000);
        m.recordBytes(null, client("alice", "c1"), 10, 1000);
        assertEquals(0, m.trackedKeys());
        assertEquals(0, metricCount());
    }

    @Test
    public void testIdleKeyIsRemovedAfterTtlAndRestartsFromZero() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, false, false, 1000);
        m.recordBytes("t1", client("a", "c"), 5, 0);
        m.recordBytes("t1", client("b", "c"), 7, 900);
        assertEquals(2, m.trackedKeys());

        // a has been idle for 1000 ms, b only for 100 ms
        m.recordBytes("t1", client("b", "c"), 1, 1000);
        assertEquals(1, m.trackedKeys());
        assertNull(total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "a")));
        assertEquals(8.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "b")));

        m.recordBytes("t1", client("a", "c"), 2, 1100);
        assertEquals(2.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "a")));
    }

    @Test
    public void testActiveKeyIsNotRemoved() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, false, false, 1000);
        for (long now = 0; now <= 5000; now += 500) {
            m.recordBytes("t1", client("a", "c"), 1, now);
        }
        assertEquals(11.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "a")));
    }

    @Test
    public void testClearRemovesAllMetrics() {
        RemoteFetchPrincipalMetrics m = new RemoteFetchPrincipalMetrics(metrics, false, false, TTL_MS);
        m.recordBytes("t1", client("a", "c"), 1, 1000);
        m.clear();

        assertEquals(0, m.trackedKeys());
        assertEquals(0, metricCount());
        m.recordBytes("t1", client("a", "c"), 5, 2000);
        assertEquals(5.0, total(RemoteFetchPrincipalMetrics.BYTE_TOTAL, Map.of("user", "a")));
    }
}
