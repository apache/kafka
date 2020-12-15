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
package org.apache.kafka.clients.admin;

import static java.util.stream.Collectors.joining;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Encapsulates details about finalized as well as supported features. This is particularly useful
 * to hold the result returned by the {@link Admin#describeFeatures(DescribeFeaturesOptions)} API.
 */
public class FeatureMetadata {

    private final Map<String, FinalizedVersionRange> finalizedFeatures;

    private final Optional<Long> finalizedFeaturesEpoch;

    private final Map<String, SupportedVersionRange> supportedFeatures;

    FeatureMetadata(final Map<String, FinalizedVersionRange> finalizedFeatures,
                           final Optional<Long> finalizedFeaturesEpoch,
                           final Map<String, SupportedVersionRange> supportedFeatures) {
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        this.supportedFeatures = new HashMap<>(supportedFeatures);
    }

    /**
     * Returns a map of finalized feature versions. Each entry in the map contains a key being a
     * feature name and the value being a range of version levels supported by every broker in the
     * cluster.
     */
    public Map<String, FinalizedVersionRange> finalizedFeatures() {
        return new HashMap<>(finalizedFeatures);
    }

    /**
     * The epoch for the finalized features.
     * If the returned value is empty, it means the finalized features are absent/unavailable.
     */
    public Optional<Long> finalizedFeaturesEpoch() {
        return finalizedFeaturesEpoch;
    }

    /**
     * Returns a map of supported feature versions. Each entry in the map contains a key being a
     * feature name and the value being a range of versions supported by a particular broker in the
     * cluster.
     */
    public Map<String, SupportedVersionRange> supportedFeatures() {
        return new HashMap<>(supportedFeatures);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeatureMetadata)) {
            return false;
        }

        final FeatureMetadata that = (FeatureMetadata) other;
        return Objects.equals(this.finalizedFeatures, that.finalizedFeatures) &&
            Objects.equals(this.finalizedFeaturesEpoch, that.finalizedFeaturesEpoch) &&
            Objects.equals(this.supportedFeatures, that.supportedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures);
    }

    private static <ValueType> String mapToString(final Map<String, ValueType> featureVersionsMap) {
        return String.format(
            "{%s}",
            featureVersionsMap
                .entrySet()
                .stream()
                .map(entry -> String.format("(%s -> %s)", entry.getKey(), entry.getValue()))
                .collect(joining(", "))
        );
    }

    @Override
    public String toString() {
        return String.format(
            "FeatureMetadata{finalizedFeatures:%s, finalizedFeaturesEpoch:%s, supportedFeatures:%s}",
            mapToString(finalizedFeatures),
            finalizedFeaturesEpoch.map(Object::toString).orElse("<none>"),
            mapToString(supportedFeatures));
    }
}
