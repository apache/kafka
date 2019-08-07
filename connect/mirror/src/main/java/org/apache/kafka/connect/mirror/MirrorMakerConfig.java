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
import java.util.Set;
import java.util.HashSet;
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

    private static final String NAME = "name";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    private static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    private static final String GROUP_ID_CONFIG = "group.id";
    private static final String OFFSET_STORAGE_TOPIC_CONFIG = "offset.storage.topic";
    private static final String OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG =
            "offset.storage.replication.factor";
    private static final String STATUS_STORAGE_TOPIC_CONFIG = "status.storage.topic";
    private static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG =
            "status.storage.replication.factor";
    private static final String CONFIG_TOPIC_CONFIG = "config.storage.topic";
    private static final String CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG =
            "config.storage.replication.factor";
    private static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    private static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    private static final String HEADER_CONVERTER_CLASS_CONFIG = "header.converter";
    private static final String BYTE_ARRAY_CONVERTER_CLASS =
        "org.apache.kafka.connect.converters.ByteArrayConverter";
    private static final String REPLICATION_FACTOR = "replication.factor";
    private static final String INTERNAL_TOPIC_REPLICATION_FACTOR = "internal.topic.replication.factor";

    static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";
    static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
   
    public MirrorMakerConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
    }

    public Set<String> clusters() {
        return new HashSet<>(getList(CLUSTERS_CONFIG));
    }

    public List<SourceAndTarget> clusterPairs() {
        List<SourceAndTarget> pairs = new ArrayList<>();
        Set<String> clusters = clusters();
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

    /** Construct a MirrorClientConfig from properties of the form cluster.x.y.z.
      * Use to connect to a cluster based on the MirrorMaker top-level config file.
      */
    public MirrorClientConfig clientConfig(String cluster) {
        Map<String, String> props = new HashMap<>();
        props.putAll(originalsStrings());
        props.putAll(clusterProps(cluster));
        return new MirrorClientConfig(props);
    }

    // loads properties of the form cluster.x.y.z
    Map<String, String> clusterProps(String cluster) {
        Map<String, String> props = new HashMap<>();
        Map<String, String> strings = originalsStrings();

        for (String k : MirrorClientConfig.CLIENT_CONFIG_DEF.names()) {
            String v = strings.get(k);
            if (v != null) {
                props.putIfAbsent("producer." + k, v);
                props.putIfAbsent("consumer." + k, v);
                props.putIfAbsent("admin." + k, v);
                props.putIfAbsent(k, v);
            }
        }
 
        props.putAll(stringsWithPrefix(cluster + "."));

        for (String k : MirrorClientConfig.CLIENT_CONFIG_DEF.names()) {
            String v = props.get(k);
            if (v != null) {
                props.putIfAbsent("producer." + k, v);
                props.putIfAbsent("consumer." + k, v);
                props.putIfAbsent("admin." + k, v);
            }
        }

        return props;
    }

    // loads worker configs based on properties of the form x.y.z and cluster.x.y.z 
    Map<String, String> workerConfig(SourceAndTarget sourceAndTarget) {
        Map<String, String> props = new HashMap<>();
        props.putAll(clusterProps(sourceAndTarget.target()));

        // fill in reasonable defaults
        props.putIfAbsent(GROUP_ID_CONFIG, sourceAndTarget.source() + "-mm2");
        props.putIfAbsent(OFFSET_STORAGE_TOPIC_CONFIG, "mm2-offsets."
                + sourceAndTarget.source() + ".internal");
        props.putIfAbsent(STATUS_STORAGE_TOPIC_CONFIG, "mm2-status."
                + sourceAndTarget.source() + ".internal");
        props.putIfAbsent(CONFIG_TOPIC_CONFIG, "mm2-configs."
                + sourceAndTarget.source() + ".internal");
        props.putIfAbsent(KEY_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS); 
        props.putIfAbsent(VALUE_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS); 
        props.putIfAbsent(HEADER_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS);

        // default to internal.topic.replication.factor or replication.factor for internal Connect topics
        String replicationFactor = props.getOrDefault(REPLICATION_FACTOR,
                originalsStrings().get(REPLICATION_FACTOR));
        String internalReplicationFactor = props.getOrDefault(INTERNAL_TOPIC_REPLICATION_FACTOR,
                replicationFactor);
        if (internalReplicationFactor != null) {
            props.putIfAbsent(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, internalReplicationFactor);
            props.putIfAbsent(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, internalReplicationFactor);
            props.putIfAbsent(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, internalReplicationFactor);
        }

        return props;
    }

    // loads properties of the form cluster.x.y.z and source->target.x.y.z
    Map<String, String> connectorBaseConfig(SourceAndTarget sourceAndTarget, Class connectorClass) {
        Map<String, String> props = new HashMap<>();

        props.putAll(originalsStrings());
        props.keySet().retainAll(MirrorConnectorConfig.CONNECTOR_CONFIG_DEF.names());
        
        props.putAll(withPrefix(SOURCE_CLUSTER_PREFIX, clusterProps(sourceAndTarget.source())));
        props.putAll(withPrefix(TARGET_CLUSTER_PREFIX, clusterProps(sourceAndTarget.target())));

        props.putIfAbsent(NAME, connectorClass.getSimpleName());
        props.putIfAbsent(CONNECTOR_CLASS, connectorClass.getName());
        props.putIfAbsent(SOURCE_CLUSTER_ALIAS, sourceAndTarget.source());
        props.putIfAbsent(TARGET_CLUSTER_ALIAS, sourceAndTarget.target());

        // default to replication.factor for internal MM2 topics
        String replicationFactor = props.get(REPLICATION_FACTOR);
        if (replicationFactor != null) {
            props.putIfAbsent(INTERNAL_TOPIC_REPLICATION_FACTOR, replicationFactor);
        }

        // override with connector-level properties
        props.putAll(stringsWithPrefix(sourceAndTarget.source() + "->"
            + sourceAndTarget.target() + "."));

        return props;
    }

    protected static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CLUSTERS_CONFIG, Type.LIST, Importance.HIGH, CLUSTERS_DOC)
            // security support
            .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .withClientSslSupport()
            .withClientSaslSupport();

    private Map<String, String> stringsWithPrefix(String prefix) {
        return originalsStrings().entrySet().stream()
            .filter(x -> x.getKey().startsWith(prefix))
            .collect(Collectors.toMap(x -> x.getKey().substring(prefix.length()), x -> x.getValue()));
    } 

    static Map<String, String> withPrefix(String prefix, Map<String, String> props) {
        return props.entrySet().stream()
            .collect(Collectors.toMap(x -> prefix + x.getKey(), x -> x.getValue()));
    }
}
