package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

public class UpdateFinalizedFeaturesResult {
    private final KafkaFuture<Void> future;

    public UpdateFinalizedFeaturesResult(KafkaFuture<Void> future) {
        this.future = future;
    }

    public KafkaFuture<Void> result() {
        return future;
    }
}
