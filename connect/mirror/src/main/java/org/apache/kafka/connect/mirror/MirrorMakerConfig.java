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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.converters.ByteArrayConverter;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

public class MirrorMakerConfig extends AbstractConfig {

    public static final String CLUSTERS = "clusters";
    private static final String CLUSTERS_DOC = "clusters";

    // Properties passed to internal Kafka clients
    static final ConfigDef CLIENT_CONFIG_DEF = new ConfigDef()
        .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            CommonClientConfigs.BOOTSTRAP_SERVERS_DOC) 
            // security support
        .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            Type.STRING,
            CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
            Importance.MEDIUM,
            CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .withClientSslSupport()
        .withClientSaslSupport();

    public MirrorMakerConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, true);
    }

    List<String> clusters() {
        return getList(CLUSTERS);
    }

    // loads properties of the form cluster.x.y.z
    Map<String, String> workerConfig(SourceAndTarget sourceAndTarget) {
        Map<String, Object> props = new HashMap<>();

        // fill in consumer, producer, admin configs
        props.putAll(sharedClientConfigs());

        // override with cluster-level properties
        props.putAll(originalsWithPrefix(sourceAndTarget.target() + "."));
 
        if (props.get(DistributedConfig.GROUP_ID_CONFIG) == null) {
            props.put(DistributedConfig.GROUP_ID_CONFIG, sourceAndTarget.source() + "-mm2");
        }
        if (props.get(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG) == null) {
            props.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "mm2-offsets."
                + sourceAndTarget.source() + ".internal");
        }
        if (props.get(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG) == null) {
            props.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "mm2-status."
                + sourceAndTarget.source() + ".internal");
        }
        if (props.get(DistributedConfig.CONFIG_TOPIC_CONFIG) == null) {
            props.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "mm2-configs."
                + sourceAndTarget.source() + ".internal");
        }
        if (!props.containsKey(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG)) {
            props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName()); 
        }
        if (!props.containsKey(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG)) {
            props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName()); 
        }

        return toStrings(props);
    }

    // loads properties of the form cluster.x.y.z and source->target.x.y.z
    Map<String, String> connectorBaseConfig(SourceAndTarget sourceAndTarget, Class connectorClass) {
        Map<String, Object> props = new HashMap<>();

        // fill in consumer, producer, admin configs
        props.putAll(sharedClientConfigs());

        // add any other top-level properties
        props.putAll(connectorConfigs());
 
        // override with cluster-level properties
        props.putAll(originalsWithPrefix(sourceAndTarget.source() + "."));

        if (props.get("name") == null) {
            props.put("name", connectorClass.getSimpleName());
        }
        if (props.get("connector.class") == null) {
            props.put("connector.class", connectorClass.getName());
        }
        if (props.get(MirrorConnectorConfig.SOURCE_CLUSTER_BOOTSTRAP_SERVERS) == null) {
            props.put(MirrorConnectorConfig.SOURCE_CLUSTER_BOOTSTRAP_SERVERS, bootstrapServers(sourceAndTarget.source()));
        }
        if (props.get(MirrorConnectorConfig.TARGET_CLUSTER_BOOTSTRAP_SERVERS) == null) {
            props.put(MirrorConnectorConfig.TARGET_CLUSTER_BOOTSTRAP_SERVERS, bootstrapServers(sourceAndTarget.target()));
        }
        if (props.get(MirrorConnectorConfig.SOURCE_CLUSTER_ALIAS) == null) {
            props.put(MirrorConnectorConfig.SOURCE_CLUSTER_ALIAS, sourceAndTarget.source());
        }
        if (props.get(MirrorConnectorConfig.TARGET_CLUSTER_ALIAS) == null) {
            props.put(MirrorConnectorConfig.TARGET_CLUSTER_ALIAS, sourceAndTarget.target());
        }

        // override with connector-level properties
        props.putAll(originalsWithPrefix(sourceAndTarget.source() + "->"
            + sourceAndTarget.target() + "."));

        if (MirrorMonitorConnector.class.isAssignableFrom(connectorClass)) {
            // SinkConnectors need the topics field. We parse the config to set it correctly.
            MirrorConnectorConfig connectorConfig = new MirrorConnectorConfig(props);
            props.put(SinkTask.TOPICS_CONFIG, connectorConfig.sourceHeartbeatsTopic());
        }

        return toStrings(props);
    }

    String bootstrapServers(String clusterName) {
        return originalsStrings().get(clusterName + ".bootstrap.servers");
    }
    
    protected static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CLUSTERS, Type.LIST, null, Importance.HIGH, CLUSTERS_DOC);

    private static Map<String, String> toStrings(Map<String, ?> props) {
        Map<String, String> copy = new HashMap<>();
        for (Map.Entry<String, ?> entry : props.entrySet()) {
            if (!(entry.getValue() instanceof String))
                throw new ClassCastException("Non-string value found in original settings for key " + entry.getKey() +
                        ": " + (entry.getValue() == null ? null : entry.getValue().getClass().getName()));
            copy.put(entry.getKey(), (String) entry.getValue());
        }
        return copy;
    }

    private Map<String, String> sharedClientConfigs() {
        Map<String, String> clientConfig = new HashMap<>();
        Map<String, String> values = originalsStrings();
        for (String k : CLIENT_CONFIG_DEF.names()) {
            String v = values.get(k);
            if (v != null) {
                clientConfig.put("producer." + k, v);
                clientConfig.put("consumer." + k, v);
                clientConfig.put("admin." + k, v);
                clientConfig.put(k, v);
            }
        }
        return clientConfig;
    }

    private Map<String, String> connectorConfigs() {
        Map<String, String> connectorConfig = new HashMap<>();
        Map<String, String> values = originalsStrings();
        for (String k : MirrorConnectorConfig.CONNECTOR_CONFIG_DEF.names()) {
            String v = values.get(k);
            if (v != null) {
                connectorConfig.put(k, v);
            }
        }
        return connectorConfig;
    }
}
