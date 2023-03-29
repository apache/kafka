package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ConfigurationImage;
import org.apache.kafka.image.ConfigurationsImage;

import java.util.Map;
import java.util.stream.Collectors;

public class ConfigMetadataZkWriter {

    private ConfigMigrationClient client;

    public void handleSnapshot(ConfigurationsImage configsImage, ClientQuotasImage quotasImage) {
        // Determine what broker config entries need changing
        Map<String, ConfigurationImage> snapshotBrokerConfigs = configsImage.resourceData()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().type().equals(ConfigResource.Type.BROKER))
            .collect(Collectors.toMap(entry -> entry.getKey().name(), Map.Entry::getValue));

        client.iterateConfigsForType(ConfigResource.Type.BROKER, (broker, props) -> {
            ConfigurationImage configImage = snapshotBrokerConfigs.get(broker);
            if (configImage != null) {
                if (props.equals(configImage.toProperties())) {
                    // No need to update
                    snapshotBrokerConfigs.remove(broker);
                }
            }

        });

        snapshotBrokerConfigs.forEach((broker, configImage) -> {
            client.writeConfigs(new ConfigResource(ConfigResource.Type.BROKER, broker), configImage.toMap(), null);
        });
    }

}
