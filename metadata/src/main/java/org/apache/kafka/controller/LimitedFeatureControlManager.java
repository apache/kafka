package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Map;
import java.util.Optional;

public interface LimitedFeatureControlManager {

    ControllerResult<ApiError> updateFeatures(
        Map<String, Short> updates,
        Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
        boolean validateOnly
    );

    Optional<MetadataVersion> metadataVersion();

    MetadataVersion metadataVersionOrThrow();

    void replay(FeatureLevelRecord record);

    boolean isControllerId(int nodeId);

    boolean isElrFeatureEnabled();
}
