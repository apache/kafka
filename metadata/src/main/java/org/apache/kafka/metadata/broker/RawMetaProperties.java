package org.apache.kafka.metadata.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class RawMetaProperties {
    public static final String ClusterIdKey = "cluster.id";
    public static final String BrokerIdKey = "broker.id";
    public static final String NodeIdKey = "node.id";
    public static final String VersionKey = "version";

    private Properties props;

    public RawMetaProperties(Properties props) {
        this.props = props != null ? props : new Properties();
    }

    public Optional<String> getClusterId() {
        return Optional.ofNullable(props.getProperty(ClusterIdKey));
    }

    public void setClusterId(String id) {
        props.setProperty(ClusterIdKey, id);
    }

    public Optional<Integer> getBrokerId() {
        return intValue(BrokerIdKey);
    }

    public void setBrokerId(int id) {
        props.setProperty(BrokerIdKey, Integer.toString(id));
    }

    public Optional<Integer> getNodeId() {
        return intValue(NodeIdKey);
    }

    public void setNodeId(int id) {
        props.setProperty(NodeIdKey, Integer.toString(id));
    }

    public int getVersion() {
        return intValue(VersionKey).orElse(0);
    }

    public void setVersion(int ver) {
        props.setProperty(VersionKey, Integer.toString(ver));
    }

    public Properties getProps(){
        return props;
    }

    public void requireVersion(int expectedVersion) {
        if (getVersion() != expectedVersion) {
            throw new RuntimeException("Expected version " + expectedVersion + ", but got version " + getVersion());
        }
    }

    private Optional<Integer> intValue(String key) {
        try {
            return Optional.ofNullable(props.getProperty(key)).map(Integer::parseInt);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to parse " + key + " property as an int: " + e.getMessage());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof RawMetaProperties) {
            RawMetaProperties other = (RawMetaProperties) obj;
            return props.equals(other.props);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return props.hashCode();
    }

//    @Override
//    public String toString() {
//        return "{" + props.keySet().toString() + "}";
//    }

    @Override
    public String toString() {
        List<String> keys = new ArrayList<>(props.stringPropertyNames());
        Collections.sort(keys);

        List<String> keyValuePairs = new ArrayList<>();
        for (String key : keys) {
            String value = props.getProperty(key);
            keyValuePairs.add(key + "=" + value);
        }

        return "{" + String.join(", ", keyValuePairs) + "}";
    }
}


