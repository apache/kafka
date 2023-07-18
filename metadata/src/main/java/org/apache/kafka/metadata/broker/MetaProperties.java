package org.apache.kafka.metadata.broker;

import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.metadata.broker.RawMetaProperties.BrokerIdKey;
import static org.apache.kafka.metadata.broker.RawMetaProperties.ClusterIdKey;
import static org.apache.kafka.metadata.broker.RawMetaProperties.NodeIdKey;

public class MetaProperties {
    private final String clusterId;
    private final int nodeId;

    public MetaProperties(String clusterId, int nodeId) {
        this.clusterId = clusterId;
        this.nodeId = nodeId;
    }

    public Properties toProperties() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setVersion(1);
        properties.setClusterId(clusterId);
        properties.setNodeId(nodeId);
        return properties.getProps();
    }

    public static MetaProperties parse(RawMetaProperties properties) {
        String clusterId = require(ClusterIdKey, properties.getClusterId());

        if (properties.getVersion() == 1) {
            Integer nodeId = require(NodeIdKey, properties.getNodeId());
            return new MetaProperties(clusterId, nodeId);
        } else if (properties.getVersion() == 0) {
            Integer brokerId = require(BrokerIdKey, properties.getBrokerId());
            return new MetaProperties(clusterId, brokerId);
        } else {
            throw new RuntimeException("Expected version 0 or 1, but got version " + properties.getVersion());
        }
    }

    private static <T> T require(String key, Optional<T> value) {
        return value.orElseThrow(() -> new RuntimeException("Failed to find required property " + key + "."));
    }
}
