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

import org.junit.Test;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

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
            "b.bootstrap.servers", "servers-two"));
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(new SourceAndTarget("a", "b"),
            MirrorSourceConnector.class);
        assertEquals("source.cluster.bootstrap.servers is set",
            "servers-one", connectorProps.get("source.cluster.bootstrap.servers"));
        assertEquals("target.cluster.bootstrap.servers is set",
            "servers-two", connectorProps.get("target.cluster.bootstrap.servers"));
    }

    @Test
    public void testFlowConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps(
            "clusters", "a, b",
            "a->b.producer.client.id", "client-one",
            "a->b.tasks.max", "123"));
        Map<String, String> connectorProps = mirrorConfig.connectorBaseConfig(new SourceAndTarget("a", "b"),
            MirrorSourceConnector.class);
        assertEquals("producer properties are set",
            "client-one", connectorProps.get("producer.client.id"));
        assertEquals("connector props should include tasks.max",
            "123", connectorProps.get("tasks.max"));
        MirrorConnectorConfig connectorConfig = new MirrorConnectorConfig(connectorProps);
        assertEquals("connector producer is configured",
            "client-one", connectorConfig.sourceProducerConfig().get("client.id"));
    }

    @Test
    public void testIncludesConnectorConfigProperties() {
        MirrorMakerConfig mirrorConfig = new MirrorMakerConfig(makeProps("tasks.max", "100"));
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
}
