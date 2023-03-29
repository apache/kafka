package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ClientQuotaRecord;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

public interface ConfigMigrationClient {

    void iterateClientQuotas(
        BiConsumer<List<ClientQuotaRecord.EntityData>, Map<String, Double>> quotaEntityConsumer
    );

    void iterateBrokerConfigs(BiConsumer<String, Map<String, String>> configConsumer);

    ZkMigrationLeadershipState writeConfigs(
        ConfigResource configResource,
        Map<String, String> configMap,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState writeClientQuotas(
        Map<String, String> clientQuotaEntity,
        Map<String, Double> quotas,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState deleteConfigs(
        ConfigResource configResource,
        ZkMigrationLeadershipState state
    );
}
