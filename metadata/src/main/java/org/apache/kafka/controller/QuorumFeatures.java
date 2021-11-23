package org.apache.kafka.controller;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.metadata.VersionRange;

import java.util.Map;
import java.util.Optional;

public class QuorumFeatures {
    private final ApiVersions apiVersions;
    private final Map<String, VersionRange> supportedFeatures;

    public QuorumFeatures(ApiVersions apiVersions, Map<String, VersionRange> supportedFeatures) {
        this.apiVersions = apiVersions;
        this.supportedFeatures = supportedFeatures;
    }

    Optional<VersionRange> quorumSupportedFeature(String featureName) {
        return Optional.of(VersionRange.ALL); // TODO fix this
    }


    Optional<VersionRange> localSupportedFeature(String featureName) {
        return Optional.ofNullable(supportedFeatures.get(featureName));
    }
}
