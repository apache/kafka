package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.config.ConfigResource;

import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

public interface ConfigMigrationClient {

    void iterateConfigsForType(ConfigResource.Type resourceType, BiConsumer<String, Properties> configConsumer);

    ZkMigrationLeadershipState writeConfigs(
        ConfigResource configResource,
        Map<String, String> configMap,
        ZkMigrationLeadershipState state
    );
}
