package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

public class DescribeFeaturesResult {

    private final KafkaFuture<FeatureMetadata> future;

    public DescribeFeaturesResult(KafkaFuture<FeatureMetadata> future) {
        this.future = future;
    }

    public KafkaFuture<FeatureMetadata> featureMetadata() {
        return future;
    }
}
