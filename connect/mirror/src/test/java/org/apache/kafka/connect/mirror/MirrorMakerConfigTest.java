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

import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
        assertEquals("source.cluster.bootstrap.servers is set",
            "servers-one", connectorProps.get("source.cluster.bootstrap.servers"));
        assertEquals("target.cluster.bootstrap.servers is set",
            "servers-two", connectorProps.get("target.cluster.bootstrap.servers"));
        assertEquals("top-level security.protocol is passed through to connector config",
            "SASL", connectorProps.get("security.protocol"));
    }

    @Test
    public void testReplicationConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "a->b.tasks.max", "123"));
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(new SourceAndTarget("a", "b"),
            MirrorSourceConnector.class);
        assertEquals("connector props should include tasks.max",
            "123", connectorProps.get("tasks.max"));
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
        assertEquals("replication.policy.separator is picked up in MirrorClientConfig",
            "__", aClientConfig.getString("replication.policy.separator"));
        assertEquals("replication.policy.separator is honored",
            "b__topic1", aClientConfig.replicationPolicy().formatRemoteTopic("b", "topic1"));
        assertEquals("client configs include boostrap.servers",
            "one:9092, two:9092", aClientConfig.adminConfig().get("bootstrap.servers"));
        assertEquals("client configs include security.protocol",
            "PLAINTEXT", aClientConfig.adminConfig().get("security.protocol"));
        assertEquals("producer configs include security.protocol",
            "SASL", aClientConfig.producerConfig().get("security.protocol"));
        assertFalse("unknown properties aren't included in client configs",
            aClientConfig.adminConfig().containsKey("xxx"));
        assertFalse("top-leve metrics reporters aren't included in client configs",
            aClientConfig.adminConfig().containsKey("metric.reporters"));
        assertEquals("security properties are picked up in MirrorClientConfig",
            "secret1", aClientConfig.getPassword("ssl.truststore.password").value());
        assertEquals("client configs include top-level security properties",
            "secret1", ((Password) aClientConfig.adminConfig().get("ssl.truststore.password")).value());
        assertEquals("security properties are translated from external sources",
            "secret2", aClientConfig.getPassword("ssl.key.password").value());
        assertEquals("client configs are translated from external sources",
            "secret2", ((Password) aClientConfig.adminConfig().get("ssl.key.password")).value());
        assertFalse("client configs should not include metrics reporter",
            aClientConfig.producerConfig().containsKey("metrics.reporter"));
        assertFalse("client configs should not include metrics reporter",
            bClientConfig.adminConfig().containsKey("metrics.reporter"));
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
        assertEquals("Connector properties like tasks.max should be passed through to underlying Connectors.",
            100, (int) connectorConfig.getInt("tasks.max"));
        assertEquals("Topics include should be passed through to underlying Connectors.",
            Arrays.asList("topic-1"), connectorConfig.getList("topics"));
        assertEquals("Groups include should be passed through to underlying Connectors.",
            Arrays.asList("group-2"), connectorConfig.getList("groups"));
        assertEquals("Config properties exclude should be passed through to underlying Connectors.",
            Arrays.asList("property-3"), connectorConfig.getList("config.properties.exclude"));
        assertEquals("Metrics reporters should be passed through to underlying Connectors.",
            Arrays.asList("FakeMetricsReporter"), connectorConfig.getList("metric.reporters"));
        assertEquals("Filters should be passed through to underlying Connectors.",
            "DefaultTopicFilter", connectorConfig.getClass("topic.filter.class").getSimpleName());
        assertEquals("replication policy separator should be passed through to underlying Connectors.",
            "__", connectorConfig.getString("replication.policy.separator"));
        assertFalse("Unknown properties should not be passed through to Connectors.",
            connectorConfig.originals().containsKey("xxx"));
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

        assertEquals("Topics exclude should be backwards compatible.",
                     Arrays.asList("topic3"), filterConfig.getList("topics.exclude"));

        assertEquals("Groups exclude should be backwards compatible.",
                     Arrays.asList("group-7"), connectorConfig.getList("groups.exclude"));

        assertEquals("Config properties exclude should be backwards compatible.",
                     Arrays.asList("property-3"), connectorConfig.getList("config.properties.exclude"));

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

        assertEquals("Topics exclude should be backwards compatible.",
                     Arrays.asList("topic3"), filterConfig.getList("topics.exclude"));

        assertEquals("Groups exclude should be backwards compatible.",
                     Arrays.asList("group-7"), connectorConfig.getList("groups.exclude"));
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
        assertEquals("source->target.topics should be passed through to TopicFilters.",
            Arrays.asList("topic1", "topic2"), filterConfig.getList("topics"));
        assertEquals("source->target.topics.exclude should be passed through to TopicFilters.",
            Arrays.asList("topic3"), filterConfig.getList("topics.exclude"));
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
        assertEquals("producer props should be passed through to worker producer config: " + bProps,
            "client-one", bProps.get("producer.client.id"));
        assertEquals("replication-level security props should be passed through to worker producer config",
            "SASL", bProps.get("producer.security.protocol"));
        assertEquals("replication-level security props should be passed through to worker producer config",
            "SASL", bProps.get("producer.security.protocol"));
        assertEquals("replication-level security props should be passed through to worker consumer config",
            "PLAINTEXT", bProps.get("consumer.security.protocol"));
        assertEquals("security properties should be passed through to worker config: " + bProps,
            "secret1", bProps.get("ssl.truststore.password"));
        assertEquals("security properties should be passed through to worker producer config: " + bProps,
            "secret1", bProps.get("producer.ssl.truststore.password"));
        assertEquals("security properties should be transformed in worker config",
            "secret2", bProps.get("ssl.key.password"));
        assertEquals("security properties should be transformed in worker producer config",
            "secret2", bProps.get("producer.ssl.key.password"));
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
