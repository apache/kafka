package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.image.ConfigurationsImage;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class ConfigMetadataZkWriter {

    private final ConfigMigrationClient client;

    private final BiConsumer<String, KRaftMigrationOperation> operationConsumer;

    public ConfigMetadataZkWriter(
        ConfigMigrationClient client,
        BiConsumer<String, KRaftMigrationOperation> operationConsumer
    ) {
        this.client = client;
        this.operationConsumer = operationConsumer;
    }

    public void handleSnapshot(ConfigurationsImage configsImage) {
        Set<ConfigResource> brokersToUpdate = new HashSet<>();
        client.iterateBrokerConfigs((broker, configs) -> {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, broker);
            Map<String, String> kraftProps = configsImage.configMapForResource(brokerResource);
            if (!kraftProps.equals(configs)) {
                brokersToUpdate.add(brokerResource);
            }
        });

        brokersToUpdate.forEach(brokerResource -> {
            Map<String, String> props = configsImage.configMapForResource(brokerResource);
            if (props.isEmpty()) {
                operationConsumer.accept("Delete configs for broker " + brokerResource.name(), migrationState ->
                    client.deleteConfigs(brokerResource, migrationState));
            } else {
                operationConsumer.accept("Update configs for broker " + brokerResource.name(), migrationState ->
                    client.writeConfigs(brokerResource, props, migrationState));
            }
        });
    }
}
