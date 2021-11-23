package org.apache.kafka.controller;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.metadata.MetadataVersion;
import org.apache.kafka.metadata.MetadataVersions;
import org.apache.kafka.metadata.VersionRange;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A holder class of the local node's supported feature flags as well as the ApiVersions of other nodes.
 */
public class QuorumFeatures {
    private final int nodeId;
    private final ApiVersions apiVersions;
    private final Map<String, VersionRange> supportedFeatures;
    private final List<Node> quorumNodes;

    public QuorumFeatures(int nodeId,
                          ApiVersions apiVersions,
                          Map<String, VersionRange> supportedFeatures,
                          List<Node> quorumNodes) {
        this.nodeId = nodeId;
        this.apiVersions = apiVersions;
        this.supportedFeatures = Collections.unmodifiableMap(supportedFeatures);
        this.quorumNodes = Collections.unmodifiableList(quorumNodes);
    }

    Optional<VersionRange> quorumSupportedFeature(String featureName) {
        List<VersionRange> supportedVersions = quorumNodes.stream()
            .filter(node -> node.id() != nodeId)
            .map(node -> apiVersions.get(node.idString()))
            .filter(Objects::nonNull)
            .map(apiVersion -> apiVersion.supportedFeatures().get(featureName))
            .filter(Objects::nonNull)
            .map(supportedVersionRange -> VersionRange.of(supportedVersionRange.min(), supportedVersionRange.max()))
            .collect(Collectors.toList());

        localSupportedFeature(featureName).ifPresent(supportedVersions::add);

        if (supportedVersions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(VersionRange.of(
                (short) supportedVersions.stream().mapToInt(VersionRange::min).max().getAsInt(),
                (short) supportedVersions.stream().mapToInt(VersionRange::max).min().getAsInt()
            ));
        }
    }


    Optional<VersionRange> localSupportedFeature(String featureName) {
        return Optional.ofNullable(supportedFeatures.get(featureName));
    }

    public static Map<String, VersionRange> defaultFeatures() {
        Map<String, VersionRange> features = new HashMap<>(1);
        features.put(MetadataVersion.FEATURE_NAME, VersionRange.of(MetadataVersions.V1.version(), MetadataVersions.latest().version()));
        return features;
    }
}
