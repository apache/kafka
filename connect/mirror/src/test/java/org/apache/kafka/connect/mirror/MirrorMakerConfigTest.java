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

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.metrics.FakeMetricsReporter;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MirrorMakerConfigTest {

    private Map<String, String> makeProps(String... keyValues) {
        Map<String, String> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            props.put(keyValues[i], keyValues[i + 1]);
        }
        return props;
    }

    @Test
    public void testClusterConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "a.bootstrap.servers", "servers-one",
            "b.bootstrap.servers", "servers-two",
            "security.protocol", "SASL",
            "replication.factor", "4"));
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(new SourceAndTarget("a", "b"),
            MirrorSourceConnector.class);
        assertEquals("servers-one", connectorProps.get("source.cluster.bootstrap.servers"),
            "source.cluster.bootstrap.servers is set");
        assertEquals("servers-two", connectorProps.get("target.cluster.bootstrap.servers"),
            "target.cluster.bootstrap.servers is set");
        assertEquals("SASL", connectorProps.get("security.protocol"),
            "top-level security.protocol is passed through to connector config");
    }

    @Test
    public void testReplicationConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "a->b.tasks.max", "123"));
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(new SourceAndTarget("a", "b"),
            MirrorSourceConnector.class);
        assertEquals("123", connectorProps.get("tasks.max"), "connector props should include tasks.max");
    }

    @Test
    public void testClientConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "config.providers", "fake",
            "config.providers.fake.class", FakeConfigProvider.class.getName(),
            "replication.policy.separator", "__",
            "ssl.truststore.password", "secret1",
            "ssl.key.password", "${fake:secret:password}",  // resolves to "secret2"
            "security.protocol", "SSL", 
            "a.security.protocol", "PLAINTEXT", 
            "a.producer.security.protocol", "SASL", 
            "a.bootstrap.servers", "one:9092, two:9092",
            "metrics.reporter", FakeMetricsReporter.class.getName(),
            "a.metrics.reporter", FakeMetricsReporter.class.getName(),
            "b->a.metrics.reporter", FakeMetricsReporter.class.getName(),
            "a.xxx", "yyy",
            "xxx", "zzz"));
        MirrorClientConfig aClientConfig = mirrorConfig.clientConfig("a");
        MirrorClientConfig bClientConfig = mirrorConfig.clientConfig("b");
        assertEquals("__", aClientConfig.getString("replication.policy.separator"),
            "replication.policy.separator is picked up in MirrorClientConfig");
        assertEquals("b__topic1", aClientConfig.replicationPolicy().formatRemoteTopic("b", "topic1"),
            "replication.policy.separator is honored");
        assertEquals("one:9092, two:9092", aClientConfig.adminConfig().get("bootstrap.servers"),
            "client configs include boostrap.servers");
        assertEquals("PLAINTEXT", aClientConfig.adminConfig().get("security.protocol"),
            "client configs include security.protocol");
        assertEquals("SASL", aClientConfig.producerConfig().get("security.protocol"),
            "producer configs include security.protocol");
        assertFalse(aClientConfig.adminConfig().containsKey("xxx"),
            "unknown properties aren't included in client configs");
        assertFalse(aClientConfig.adminConfig().containsKey("metric.reporters"),
            "top-leve metrics reporters aren't included in client configs");
        assertEquals("secret1", aClientConfig.getPassword("ssl.truststore.password").value(),
            "security properties are picked up in MirrorClientConfig");
        assertEquals("secret1", ((Password) aClientConfig.adminConfig().get("ssl.truststore.password")).value(),
            "client configs include top-level security properties");
        assertEquals("secret2", aClientConfig.getPassword("ssl.key.password").value(),
            "security properties are translated from external sources");
        assertEquals("secret2", ((Password) aClientConfig.adminConfig().get("ssl.key.password")).value(),
            "client configs are translated from external sources");
        assertFalse(aClientConfig.producerConfig().containsKey("metrics.reporter"),
            "client configs should not include metrics reporter");
        assertFalse(bClientConfig.adminConfig().containsKey("metrics.reporter"),
            "client configs should not include metrics reporter");
    }

    @Test
    public void testIncludesConnectorConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "tasks.max", "100",
            "topics", "topic-1",
            "groups", "group-2",
            "replication.policy.separator", "__",
            "config.properties.exclude", "property-3",
            "metric.reporters", "FakeMetricsReporter",
            "topic.filter.class", DefaultTopicFilter.class.getName(),
            "xxx", "yyy"));
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(sourceAndTarget,
            MirrorSourceConnector.class);
        MirrorConnectorConfig connectorConfig = new MirrorConnectorConfig(connectorProps);
        assertEquals(100, (int) connectorConfig.getInt("tasks.max"),
            "Connector properties like tasks.max should be passed through to underlying Connectors.");
        assertEquals(Arrays.asList("topic-1"), connectorConfig.getList("topics"),
            "Topics include should be passed through to underlying Connectors.");
        assertEquals(Arrays.asList("group-2"), connectorConfig.getList("groups"),
            "Groups include should be passed through to underlying Connectors.");
        assertEquals(Arrays.asList("property-3"), connectorConfig.getList("config.properties.exclude"),
            "Config properties exclude should be passed through to underlying Connectors.");
        assertEquals(Arrays.asList("FakeMetricsReporter"), connectorConfig.getList("metric.reporters"),
            "Metrics reporters should be passed through to underlying Connectors.");
        assertEquals("DefaultTopicFilter", connectorConfig.getClass("topic.filter.class").getSimpleName(),
            "Filters should be passed through to underlying Connectors.");
        assertEquals("__", connectorConfig.getString("replication.policy.separator"),
            "replication policy separator should be passed through to underlying Connectors.");
        assertFalse(connectorConfig.originals().containsKey("xxx"),
            "Unknown properties should not be passed through to Connectors.");
    }

    @Test
    public void testConfigBackwardsCompatibility() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "groups.blacklist", "group-7",
            "topics.blacklist", "topic3",
            "config.properties.blacklist", "property-3",
            "topic.filter.class", DefaultTopicFilter.class.getName()));
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(sourceAndTarget,
                                                                              MirrorSourceConnector.class);
        MirrorConnectorConfig connectorConfig = new MirrorConnectorConfig(connectorProps);
        DefaultTopicFilter.TopicFilterConfig filterConfig =
            new DefaultTopicFilter.TopicFilterConfig(connectorProps);

        assertEquals(Arrays.asList("topic3"), filterConfig.getList("topics.exclude"),
            "Topics exclude should be backwards compatible.");

        assertEquals(Arrays.asList("group-7"), connectorConfig.getList("groups.exclude"),
            "Groups exclude should be backwards compatible.");

        assertEquals(Arrays.asList("property-3"), connectorConfig.getList("config.properties.exclude"),
            "Config properties exclude should be backwards compatible.");

    }

    @Test
    public void testConfigBackwardsCompatibilitySourceTarget() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "source->target.topics.blacklist", "topic3",
            "source->target.groups.blacklist", "group-7",
            "topic.filter.class", DefaultTopicFilter.class.getName()));
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(sourceAndTarget,
                                                                              MirrorSourceConnector.class);
        MirrorConnectorConfig connectorConfig = new MirrorConnectorConfig(connectorProps);
        DefaultTopicFilter.TopicFilterConfig filterConfig =
            new DefaultTopicFilter.TopicFilterConfig(connectorProps);

        assertEquals(Arrays.asList("topic3"), filterConfig.getList("topics.exclude"),
            "Topics exclude should be backwards compatible.");

        assertEquals(Arrays.asList("group-7"), connectorConfig.getList("groups.exclude"),
            "Groups exclude should be backwards compatible.");
    }

    @Test
    public void testIncludesTopicFilterProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "source->target.topics", "topic1, topic2",
            "source->target.topics.exclude", "topic3"));
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(sourceAndTarget,
            MirrorSourceConnector.class);
        DefaultTopicFilter.TopicFilterConfig filterConfig = 
            new DefaultTopicFilter.TopicFilterConfig(connectorProps);
        assertEquals(Arrays.asList("topic1", "topic2"), filterConfig.getList("topics"),
            "source->target.topics should be passed through to TopicFilters.");
        assertEquals(Arrays.asList("topic3"), filterConfig.getList("topics.exclude"),
            "source->target.topics.exclude should be passed through to TopicFilters.");
    }

    @Test
    public void testWorkerConfigs() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "config.providers", "fake",
            "config.providers.fake.class", FakeConfigProvider.class.getName(),
            "replication.policy.separator", "__",
            "offset.storage.replication.factor", "123",
            "b.status.storage.replication.factor", "456",
            "b.producer.client.id", "client-one",
            "b.security.protocol", "PLAINTEXT",
            "b.producer.security.protocol", "SASL",
            "ssl.truststore.password", "secret1",
            "ssl.key.password", "${fake:secret:password}",  // resolves to "secret2"
            "b.xxx", "yyy"));
        SourceAndTarget a = new SourceAndTarget("b", "a");
        SourceAndTarget b = new SourceAndTarget("a", "b");
        Map<String, String> aProps = mirrorConfig.workerConfig(a);
        assertEquals("123", aProps.get("offset.storage.replication.factor"));
        Map<String, String> bProps = mirrorConfig.workerConfig(b);
        assertEquals("456", bProps.get("status.storage.replication.factor"));
        assertEquals("client-one", bProps.get("producer.client.id"),
            "producer props should be passed through to worker producer config: " + bProps);
        assertEquals("SASL", bProps.get("producer.security.protocol"),
            "replication-level security props should be passed through to worker producer config");
        assertEquals("SASL", bProps.get("producer.security.protocol"),
            "replication-level security props should be passed through to worker producer config");
        assertEquals("PLAINTEXT", bProps.get("consumer.security.protocol"),
            "replication-level security props should be passed through to worker consumer config");
        assertEquals("secret1", bProps.get("ssl.truststore.password"),
            "security properties should be passed through to worker config: " + bProps);
        assertEquals("secret1", bProps.get("producer.ssl.truststore.password"),
            "security properties should be passed through to worker producer config: " + bProps);
        assertEquals("secret2", bProps.get("ssl.key.password"),
            "security properties should be transformed in worker config");
        assertEquals("secret2", bProps.get("producer.ssl.key.password"),
            "security properties should be transformed in worker producer config");
    }

    @Test
    public void testClusterPairsWithDefaultSettings() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
                "clusters", "a, b, c"));
        // implicit configuration associated
        // a->b.enabled=false
        // a->b.emit.heartbeat.enabled=true
        // a->c.enabled=false
        // a->c.emit.heartbeat.enabled=true
        // b->a.enabled=false
        // b->a.emit.heartbeat.enabled=true
        // b->c.enabled=false
        // b->c.emit.heartbeat.enabled=true
        // c->a.enabled=false
        // c->a.emit.heartbeat.enabled=true
        // c->b.enabled=false
        // c->b.emit.heartbeat.enabled=true
        List<SourceAndTarget> clusterPairs = mirrorConfig.clusterPairs();
        assertEquals(6, clusterPairs.size(), "clusterPairs count should match all combinations count");
    }

    @Test
    public void testEmptyClusterPairsWithGloballyDisabledHeartbeats() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
                "clusters", "a, b, c",
                "emit.heartbeats.enabled", "false"));
        assertEquals(0, mirrorConfig.clusterPairs().size(), "clusterPairs count should be 0");
    }

    @Test
    public void testClusterPairsWithTwoDisabledHeartbeats() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
                "clusters", "a, b, c",
                "a->b.emit.heartbeats.enabled", "false",
                "a->c.emit.heartbeats.enabled", "false"));
        List<SourceAndTarget> clusterPairs = mirrorConfig.clusterPairs();
        assertEquals(4, clusterPairs.size(),
            "clusterPairs count should match all combinations count except x->y.emit.heartbeats.enabled=false");
    }

    @Test
    public void testClusterPairsWithGloballyDisabledHeartbeats() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
                "clusters", "a, b, c, d, e, f",
                "emit.heartbeats.enabled", "false",
                "a->b.enabled", "true",
                "a->c.enabled", "true",
                "a->d.enabled", "true",
                "a->e.enabled", "false",
                "a->f.enabled", "false"));
        List<SourceAndTarget> clusterPairs = mirrorConfig.clusterPairs();
        assertEquals(3, clusterPairs.size(),
            "clusterPairs count should match (x->y.enabled=true or x->y.emit.heartbeats.enabled=true) count");

        // Link b->a.enabled doesn't exist therefore it must not be in clusterPairs
        SourceAndTarget sourceAndTarget = new SourceAndTarget("b", "a");
        assertFalse(clusterPairs.contains(sourceAndTarget), "disabled/unset link x->y should not be in clusterPairs");
    }

    @Test
    public void testClusterPairsWithGloballyDisabledHeartbeatsCentralLocal() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
                "clusters", "central, local_one, local_two, beats_emitter",
                "emit.heartbeats.enabled", "false",
                "central->local_one.enabled", "true",
                "central->local_two.enabled", "true",
                "beats_emitter->central.emit.heartbeats.enabled", "true"));

        assertEquals(3, mirrorConfig.clusterPairs().size(),
            "clusterPairs count should match (x->y.enabled=true or x->y.emit.heartbeats.enabled=true) count");
    }

    public static class FakeConfigProvider implements ConfigProvider {

        Map<String, String> secrets = Collections.singletonMap("password", "secret2");

        @Override
        public void configure(Map<String, ?> props) {
        }

        @Override
        public void close() {
        }

        @Override
        public ConfigData get(String path) {
            return new ConfigData(secrets);
        }

        @Override
        public ConfigData get(String path, Set<String> keys) {
            return get(path);
        }
    }
}
