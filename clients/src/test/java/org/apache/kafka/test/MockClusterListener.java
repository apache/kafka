package org.apache.kafka.test;

import org.apache.kafka.common.ClusterListener;
import org.apache.kafka.common.ClusterResourceMeta;

public class MockClusterListener implements ClusterListener {


    private ClusterResourceMeta clusterResourceMeta;

    @Override
    public void onClusterUpdate(ClusterResourceMeta clusterMetadata) {
        this.clusterResourceMeta = clusterMetadata;
    }

    public ClusterResourceMeta getClusterResourceMeta() {
        return clusterResourceMeta;
    }
}
