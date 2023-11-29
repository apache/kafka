package org.apache.kafka.streams.internals;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.ClientInstanceIds;

import java.util.HashMap;
import java.util.Map;

public class ClientInstanceIdsImpl implements ClientInstanceIds {
    private final Map<String, Uuid> consumerInstanceIds = new HashMap<>();
    private final Map<String, Uuid> producerInstanceIds = new HashMap<>();
    private Uuid adminInstanceId;

    public void addConsumerInstanceId(final String key, final Uuid instanceId) {
        consumerInstanceIds.put(key, instanceId);
    }

    public void addProducerInstanceId(final String key, final Uuid instanceId) {
        producerInstanceIds.put(key, instanceId);
    }

    public void setAdminInstanceId(final Uuid instanceId) {
        adminInstanceId = instanceId;
    }

    @Override
    public Uuid adminInstanceId() {
        return adminInstanceId;
    }

    @Override
    public Map<String, Uuid> consumerInstanceIds() {
        return consumerInstanceIds;
    }

    @Override
    public Map<String, Uuid> producerInstanceIds() {
        return producerInstanceIds;
    }
}
