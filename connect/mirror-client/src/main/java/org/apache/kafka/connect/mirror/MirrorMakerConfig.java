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

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

/** Top-level config describing replication flows between multiple Kafka clusters.
 *
 *  Supports cluster-level properties of the form cluster.x.y.z, and flow-level
 *  properties of the form source->target.x.y.z.
 *  e.g.
 *
 *      clusters = A, B, C
 *      A.bootstrap.servers = aaa:9092
 *      A.security.protocol = SSL
 *      --->%---
 *      A->B.enabled = true
 *      A->B.producer.client.id = "A-B-producer"
 *      --->%---
 *
 */
public class MirrorMakerConfig extends AbstractConfig {

    public static final String CLUSTERS_CONFIG = "clusters";
    private static final String CLUSTERS_DOC = "List of cluster aliases.";

    public static final String ENABLED_CONFIG = "enabled";
    private static final String ENABLED_DOC = "Whether to replicate source->target.";

    private static final String NAME = "name";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    private static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    private static final String GROUP_ID_CONFIG = "group.id";
    private static final String OFFSET_STORAGE_TOPIC_CONFIG = "offset.storage.topic";
    private static final String STATUS_STORAGE_TOPIC_CONFIG = "status.storage.topic";
    private static final String CONFIG_TOPIC_CONFIG = "config.storage.topic";
    private static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    private static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    private static final String BYTE_ARRAY_CONVERTER_CLASS =
        "org.apache.kafka.connect.converters.ByteArrayConverter";

    static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";
    static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
    
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

    static final ConfigDef ENABLED_CONFIG_DEF = new ConfigDef()
        .define(ENABLED_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.HIGH,
            ENABLED_DOC);

    public MirrorMakerConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, false);
    }

    public List<String> clusters() {
        return getList(CLUSTERS_CONFIG);
    }

    public List<SourceAndTarget> enabledClusterPairs() {
        return clusterPairs().stream()
            .filter(this::enabled)
            .collect(Collectors.toList());
    }

    public List<SourceAndTarget> clusterPairs() {
        List<SourceAndTarget> pairs = new ArrayList<>();
        List<String> clusters = clusters();
        for (String source : clusters) {
            for (String target : clusters) {
                SourceAndTarget sourceAndTarget = new SourceAndTarget(source, target);
                if (!source.equals(target)) {
                    pairs.add(sourceAndTarget);
                }
            }
        }
        return pairs;
    }

    boolean enabled(SourceAndTarget sourceAndTarget) {
        return new AbstractConfig(ENABLED_CONFIG_DEF, originalsWithPrefix(
            sourceAndTarget.source() + "->" + sourceAndTarget.target() + "."), false)
            .getBoolean(ENABLED_CONFIG);
    }

    /** Construct a MirrorClientConfig from properties of the form cluster.x.y.z.
      * Use to connect to a cluster based on the MirrorMaker top-level config file.
      */
    public MirrorClientConfig clientConfig(String cluster) {
        return new MirrorClientConfig(clusterProps(cluster));
    }

    // loads properties of the form cluster.x.y.z
    Map<String, String> clusterProps(String cluster) {
        Map<String, Object> props = new HashMap<>();

        // fill in consumer, producer, admin configs
        props.putAll(sharedClientConfigs());

        // override with cluster-level properties
        props.putAll(originalsWithPrefix(cluster + "."));

        return toStrings(props);
    }

    // loads worker configs based on properties of the form cluster.x.y.z 
    Map<String, String> workerConfig(SourceAndTarget sourceAndTarget) {
        Map<String, String> props = clusterProps(sourceAndTarget.target());

        if (props.get(GROUP_ID_CONFIG) == null) {
            props.put(GROUP_ID_CONFIG, sourceAndTarget.source() + "-mm2");
        }
        if (props.get(OFFSET_STORAGE_TOPIC_CONFIG) == null) {
            props.put(OFFSET_STORAGE_TOPIC_CONFIG, "mm2-offsets."
                + sourceAndTarget.source() + ".internal");
        }
        if (props.get(STATUS_STORAGE_TOPIC_CONFIG) == null) {
            props.put(STATUS_STORAGE_TOPIC_CONFIG, "mm2-status."
                + sourceAndTarget.source() + ".internal");
        }
        if (props.get(CONFIG_TOPIC_CONFIG) == null) {
            props.put(CONFIG_TOPIC_CONFIG, "mm2-configs."
                + sourceAndTarget.source() + ".internal");
        }
        if (props.get(SOURCE_CLUSTER_ALIAS) == null) {
            props.put(SOURCE_CLUSTER_ALIAS, sourceAndTarget.source());
        }
        if (props.get(TARGET_CLUSTER_ALIAS) == null) {
            props.put(TARGET_CLUSTER_ALIAS, sourceAndTarget.target());
        }
        if (!props.containsKey(KEY_CONVERTER_CLASS_CONFIG)) {
            props.put(KEY_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS); 
        }
        if (!props.containsKey(VALUE_CONVERTER_CLASS_CONFIG)) {
            props.put(VALUE_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS); 
        }

        return props;
    }

    // loads properties of the form cluster.x.y.z and source->target.x.y.z
    Map<String, String> connectorBaseConfig(SourceAndTarget sourceAndTarget, Class connectorClass) {
        Map<String, String> props = new HashMap<>();

        props.putAll(originalsStrings());
        
        props.putAll(withPrefix(SOURCE_CLUSTER_PREFIX, clusterProps(sourceAndTarget.source())));
        props.putAll(withPrefix(TARGET_CLUSTER_PREFIX, clusterProps(sourceAndTarget.target())));

        if (props.get(NAME) == null) {
            props.put(NAME, connectorClass.getSimpleName());
        }
        if (props.get(CONNECTOR_CLASS) == null) {
            props.put(CONNECTOR_CLASS, connectorClass.getName());
        }
        if (props.get(SOURCE_CLUSTER_ALIAS) == null) {
            props.put(SOURCE_CLUSTER_ALIAS, sourceAndTarget.source());
        }
        if (props.get(TARGET_CLUSTER_ALIAS) == null) {
            props.put(TARGET_CLUSTER_ALIAS, sourceAndTarget.target());
        }

        // override with connector-level properties
        props.putAll(toStrings(originalsWithPrefix(sourceAndTarget.source() + "->"
            + sourceAndTarget.target() + ".")));

        if (!enabled(sourceAndTarget)) {
            props.put(ENABLED_CONFIG, "false");
        }

        return props;
    }

    protected static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CLUSTERS_CONFIG, Type.LIST, null, Importance.HIGH, CLUSTERS_DOC);

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

    static Map<String, String> withPrefix(String prefix, Map<String, String> props) {
        return props.entrySet().stream()
            .collect(Collectors.toMap(x -> prefix + x.getKey(), x -> x.getValue()));
    }
}
