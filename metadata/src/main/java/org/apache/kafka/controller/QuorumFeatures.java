/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A holder class of the local node's supported feature flags as well as a supplier of the current
 * voter IDs.
 */
public final class QuorumFeatures {
    public static final VersionRange DISABLED = VersionRange.of(0, 0);

    private final int nodeId;
    private final Map<String, VersionRange> localSupportedFeatures;
    private final Supplier<Set<Integer>> votersSupplier;

    public static Optional<String> reasonNotSupported(
        short newVersion,
        String what,
        VersionRange range
    ) {
        if (!range.contains(newVersion)) {
            if (range.max() == (short) 0) {
                return Optional.of(what + " does not support this feature.");
            } else {
                return Optional.of(what + " only supports versions " + range);
            }
        }
        return Optional.empty();
    }

    public static Map<String, VersionRange> defaultSupportedFeatureMap(boolean enableUnstable) {
        Map<String, VersionRange> features = new HashMap<>(1);
        features.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
                MetadataVersion.MINIMUM_VERSION.featureLevel(),
                enableUnstable ?
                    MetadataVersion.latestTesting().featureLevel() :
                    MetadataVersion.latestProduction().featureLevel()));
        for (Feature feature : Feature.PRODUCTION_FEATURES) {
            short maxVersion = enableUnstable ? feature.latestTesting() : feature.latestProduction();
            if (maxVersion > 0) {
                features.put(feature.featureName(), VersionRange.of(feature.minimumProduction(), maxVersion));
            }
        }
        return features;
    }

    public QuorumFeatures(
        int nodeId,
        Map<String, VersionRange> localSupportedFeatures,
        Supplier<Set<Integer>> votersSupplier
    ) {
        this.nodeId = nodeId;
        this.localSupportedFeatures = Collections.unmodifiableMap(localSupportedFeatures);
        this.votersSupplier = votersSupplier;
    }

    public int nodeId() {
        return nodeId;
    }

    /**
     * Returns the IDs of the nodes which are currently part of the voter set.
     */
    public Set<Integer> voterIds() {
        return votersSupplier.get();
    }

    public VersionRange localSupportedFeature(String name) {
        return localSupportedFeatures.getOrDefault(name, DISABLED);
    }

    /**
     * Returns true if the given node ID is currently part of the voter set.
     */
    public boolean isVoterId(int nodeId) {
        return voterIds().contains(nodeId);
    }

    public Optional<String> reasonNotLocallySupported(
        String featureName,
        short newVersion
    ) {
        return reasonNotSupported(newVersion,
            "Local controller " + nodeId,
            localSupportedFeature(featureName));
    }

    /*
     * The set of voters is not part of the identity of this object because it can change at any
     * time when the cluster supports dynamic quorums.
     */
    @Override
    public int hashCode() {
        return Objects.hash(nodeId, localSupportedFeatures);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(QuorumFeatures.class))) return false;
        QuorumFeatures other = (QuorumFeatures) o;
        return nodeId == other.nodeId &&
            localSupportedFeatures.equals(other.localSupportedFeatures);
    }

    @Override
    public String toString() {
        List<String> features = new ArrayList<>();
        localSupportedFeatures.forEach((key, value) -> features.add(key + ": " + value));
        features.sort(String::compareTo);
        return "QuorumFeatures" +
            "(nodeId=" + nodeId +
            ", localSupportedFeatures={" + features + "}" +
            ")";
    }
}
