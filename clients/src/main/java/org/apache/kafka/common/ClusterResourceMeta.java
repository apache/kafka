package org.apache.kafka.common;

public class ClusterResourceMeta {

    private final String clusterId;

    public ClusterResourceMeta(String clusterId) {

        this.clusterId = clusterId;
    }

    public String getClusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "ClusterResourceMeta{" +
                "clusterId='" + clusterId + '\'' +
                '}';
    }
}
