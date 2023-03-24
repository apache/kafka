package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

class CapturingMigrationClient implements MigrationClient {

    private final Set<Integer> brokerIds;
    private final TopicMigrationClient topicMigrationClient;

    public final Map<ConfigResource, Map<String, String>> capturedConfigs = new HashMap<>();

    public CapturingMigrationClient(
        Set<Integer> brokerIdsInZk,
        TopicMigrationClient topicMigrationClient
    ) {
        this.brokerIds = brokerIdsInZk;
        this.topicMigrationClient = topicMigrationClient;
    }

    @Override
    public ZkMigrationLeadershipState getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState initialState) {
        return initialState;
    }

    @Override
    public ZkMigrationLeadershipState setMigrationRecoveryState(ZkMigrationLeadershipState state) {
        return state;
    }

    @Override
    public ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state) {
        return state;
    }

    @Override
    public ZkMigrationLeadershipState releaseControllerLeadership(ZkMigrationLeadershipState state) {
        return state;
    }


    @Override
    public TopicMigrationClient topicClient() {
        return topicMigrationClient;
    }

    @Override
    public ZkMigrationLeadershipState writeConfigs(
            ConfigResource configResource,
            Map<String, String> configMap,
            ZkMigrationLeadershipState state
    ) {
        capturedConfigs.computeIfAbsent(configResource, __ -> new HashMap<>()).putAll(configMap);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState writeClientQuotas(
            Map<String, String> clientQuotaEntity,
            Map<String, Double> quotas,
            ZkMigrationLeadershipState state
    ) {
        return state;
    }

    @Override
    public ZkMigrationLeadershipState writeProducerId(
            long nextProducerId,
            ZkMigrationLeadershipState state
    ) {
        return state;
    }

    @Override
    public void readAllMetadata(
            Consumer<List<ApiMessageAndVersion>> batchConsumer,
            Consumer<Integer> brokerIdConsumer
    ) {

    }

    @Override
    public Set<Integer> readBrokerIds() {
        return brokerIds;
    }

    @Override
    public Set<Integer> readBrokerIdsFromTopicAssignments() {
        return brokerIds;
    }
}
