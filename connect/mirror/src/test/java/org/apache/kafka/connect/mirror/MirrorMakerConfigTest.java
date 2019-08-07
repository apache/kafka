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

import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.common.config.types.Password;

import org.junit.Test;

import java.util.Map;
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
            "replication.factor", "4"));
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(new SourceAndTarget("a", "b"),
            MirrorSourceConnector.class);
        assertEquals("source.cluster.bootstrap.servers is set",
            "servers-one", connectorProps.get("source.cluster.bootstrap.servers"));
        assertEquals("target.cluster.bootstrap.servers is set",
            "servers-two", connectorProps.get("target.cluster.bootstrap.servers"));
        assertEquals("internal.topic.replication.factor is set based on replication.factor",
            "4", connectorProps.get("internal.topic.replication.factor"));
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
            "replication.policy.separator", "__",
            "ssl.truststore.password", "secret",
            "a.security.protocol", "PLAINTEXT", 
            "a.producer.security.protocol", "SASL", 
            "a.bootstrap.servers", "one:9092, two:9092", 
            "a.xxx", "yyy"));
        MirrorClientConfig clientConfig = mirrorConfig.clientConfig("a");
        assertEquals("replication.policy.separator is picked up in MirrorClientConfig",
            "__", clientConfig.getString("replication.policy.separator"));
        assertEquals("replication.policy.separator is honored",
            "b__topic1", clientConfig.replicationPolicy().formatRemoteTopic("b", "topic1"));
        Map<String, Object> adminProps = clientConfig.adminConfig();
        assertEquals("client configs include boostrap.servers",
            "one:9092, two:9092", adminProps.get("bootstrap.servers"));
        assertEquals("client configs include security.protocol",
            "PLAINTEXT", adminProps.get("security.protocol"));
        assertEquals("producer configs include security.protocol",
            "SASL", clientConfig.producerConfig().get("security.protocol"));
        assertFalse("unknown properties aren't included in client configs",
            adminProps.containsKey("xxx"));
        assertEquals("security properties are picked up in MirrorClientConfig",
            "secret", clientConfig.getPassword("ssl.truststore.password").value());
        assertEquals("client configs include top-level security properties",
            "secret", ((Password) adminProps.get("ssl.truststore.password")).value());
    }

    @Test
    public void testIncludesConnectorConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "tasks.max", "100"));
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(sourceAndTarget,
            MirrorSourceConnector.class);
        ConnectorConfig connectorConfig = new ConnectorConfig(new Plugins(connectorProps), connectorProps);
        assertEquals("Connector properties like tasks.max should be passed through to underlying Connectors.",
            100, (int) connectorConfig.getInt("tasks.max"));
    }

    @Test
    public void testIncludesTopicFilterProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "source->target.topics", "topic1, topic2",
            "source->target.topics.blacklist", "topic3"));
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(sourceAndTarget,
            MirrorSourceConnector.class);
        DefaultTopicFilter.TopicFilterConfig filterConfig = 
            new DefaultTopicFilter.TopicFilterConfig(connectorProps);
        assertEquals("source->target.topics should be passed through to TopicFilters.",
            Arrays.asList("topic1", "topic2"), filterConfig.getList("topics"));
        assertEquals("source->target.topics.blacklist should be passed through to TopicFilters.",
            Arrays.asList("topic3"), filterConfig.getList("topics.blacklist"));
    }

    @Test
    public void testWorkerConfigs() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "replication.factor", "123",
            "b.replication.factor", "456",
            "b.producer.client.id", "client-one",
            "b.security.protocol", "PLAINTEXT",
            "b.producer.security.protocol", "SASL",
            "ssl.truststore.password", "secret"));
        SourceAndTarget a = new SourceAndTarget("b", "a");
        SourceAndTarget b = new SourceAndTarget("a", "b");
        Map<String, String> aProps = mirrorConfig.workerConfig(a);
        assertEquals("123", aProps.get("offset.storage.replication.factor"));
        assertEquals("123", aProps.get("status.storage.replication.factor"));
        assertEquals("123", aProps.get("config.storage.replication.factor"));
        Map<String, String> bProps = mirrorConfig.workerConfig(b);
        assertEquals("456", bProps.get("offset.storage.replication.factor"));
        assertEquals("456", bProps.get("status.storage.replication.factor"));
        assertEquals("456", bProps.get("config.storage.replication.factor"));
        assertEquals("producer props should be passed through to worker producer config: " + bProps,
            "client-one", bProps.get("producer.client.id"));
        assertEquals("replication-level security props should be passed through to worker producer config",
            "SASL", bProps.get("producer.security.protocol"));
        assertEquals("replication-level security props should be passed through to worker producer config",
            "SASL", bProps.get("producer.security.protocol"));
        assertEquals("replication-level security props should be passed through to worker consumer config",
            "PLAINTEXT", bProps.get("consumer.security.protocol"));
        assertEquals("security properties should be passed through to worker config: " + bProps,
            "secret", bProps.get("ssl.truststore.password"));
        assertEquals("security properties should be passed through to worker producer config: " + bProps,
            "secret", bProps.get("producer.ssl.truststore.password"));
    }

}
