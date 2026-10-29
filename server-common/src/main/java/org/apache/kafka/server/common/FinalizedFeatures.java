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
package org.apache.kafka.server.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the finalized feature levels for a Kafka cluster.
 * <p>
 * This class can be in one of three states:
 * <ul>
 *   <li>Unknown - the metadata version has not been committed yet, e.g. before a quorum is
 *       formed. Used as the initial state in {@code FeaturesPublisher}. (use {@link #unknown()})</li>
 *   <li>Metadata version only - the metadata version is known but no additional features or
 *       epoch have been set. Used when only the metadata version needs to be represented
 *       without a full set of finalized features. (use {@link #fromMetadataVersion(MetadataVersion)})</li>
 *   <li>Full features - metadata version, features map, and epoch are all known. Used after
 *       the controller has committed feature records. (use {@link #of(MetadataVersion, Map, long)})</li>
 * </ul>
 */
public final class FinalizedFeatures {
    private static final FinalizedFeatures UNKNOWN = new FinalizedFeatures(Optional.empty(), Map.of(), -1);

    private final Optional<MetadataVersion> metadataVersion;
    private final Map<String, Short> finalizedFeatures;
    private final long finalizedFeaturesEpoch;

    private FinalizedFeatures(
        Optional<MetadataVersion> metadataVersion,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch
    ) {
        this.metadataVersion = metadataVersion;
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        metadataVersion.ifPresent(mv ->
            this.finalizedFeatures.put(MetadataVersion.FEATURE_NAME, mv.featureLevel()));
    }

    /**
     * Returns a sentinel value representing unknown finalized features.
     *
     * @return the unknown finalized features instance
     */
    public static FinalizedFeatures unknown() {
        return UNKNOWN;
    }

    /**
     * Creates a new instance from the given KRaft metadata version.
     *
     * @param version the metadata version
     * @return a new FinalizedFeatures instance
     * @throws NullPointerException if version is null
     */
    public static FinalizedFeatures fromMetadataVersion(MetadataVersion version) {
        Objects.requireNonNull(version, "version cannot be null");
        return new FinalizedFeatures(Optional.of(version), Map.of(), -1);
    }

    /**
     * Creates a new instance with the given metadata version, features map, and epoch.
     *
     * @param metadataVersion the metadata version
     * @param finalizedFeatures the map of feature names to their finalized levels
     * @param epoch the epoch of the finalized features
     * @return a new FinalizedFeatures instance
     * @throws NullPointerException if metadataVersion or finalizedFeatures is null
     */
    public static FinalizedFeatures of(MetadataVersion metadataVersion, Map<String, Short> finalizedFeatures, long epoch) {
        Objects.requireNonNull(metadataVersion, "metadataVersion cannot be null");
        Objects.requireNonNull(finalizedFeatures, "finalizedFeatures cannot be null");
        return new FinalizedFeatures(Optional.of(metadataVersion), finalizedFeatures, epoch);
    }

    /**
     * Returns whether the finalized features are unknown.
     *
     * @return true if the finalized features are unknown, false otherwise
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    /**
     * Returns the metadata version, throwing an exception if unknown.
     *
     * @return the metadata version
     * @throws IllegalStateException if the metadata version is unknown
     */
    public MetadataVersion metadataVersionOrThrow() {
        return metadataVersion.orElseThrow(() ->
            new IllegalStateException("Metadata version is unknown"));
    }

    /**
     * Returns the map of feature names to their finalized levels.
     *
     * @return the finalized features map
     */
    public Map<String, Short> finalizedFeatures() {
        return finalizedFeatures;
    }

    /**
     * Returns the epoch of the finalized features.
     *
     * @return the finalized features epoch
     */
    public long finalizedFeaturesEpoch() {
        return finalizedFeaturesEpoch;
    }

    /**
     * Creates a new instance with the specified feature level set or removed.
     * If level is 0, the feature is removed. Otherwise, the feature is set to the given level.
     *
     * @param key the feature name
     * @param level the feature level (0 to remove)
     * @return a new FinalizedFeatures instance with the updated feature level
     * @throws IllegalStateException if this is the unknown instance
     */
    public FinalizedFeatures setFinalizedLevel(String key, short level) {
        if (isUnknown()) {
            throw new IllegalStateException("Cannot set finalized level on unknown FinalizedFeatures");
        }
        if (level == (short) 0) {
            if (finalizedFeatures.containsKey(key)) {
                Map<String, Short> newFinalizedFeatures = new HashMap<>(finalizedFeatures);
                newFinalizedFeatures.remove(key);
                return new FinalizedFeatures(
                    metadataVersion,
                    newFinalizedFeatures,
                    finalizedFeaturesEpoch);
            } else {
                return this;
            }
        } else {
            Map<String, Short> newFinalizedFeatures = new HashMap<>(finalizedFeatures);
            newFinalizedFeatures.put(key, level);
            return new FinalizedFeatures(
                metadataVersion,
                newFinalizedFeatures,
                finalizedFeaturesEpoch);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FinalizedFeatures that = (FinalizedFeatures) o;
        return finalizedFeaturesEpoch == that.finalizedFeaturesEpoch &&
                Objects.equals(metadataVersion, that.metadataVersion) &&
                Objects.equals(finalizedFeatures, that.finalizedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataVersion, finalizedFeatures, finalizedFeaturesEpoch);
    }

    @Override
    public String toString() {
        return "FinalizedFeatures(" +
               "metadataVersion=" + metadataVersion +
               ", finalizedFeatures=" + finalizedFeatures +
               ", finalizedFeaturesEpoch=" + finalizedFeaturesEpoch +
               ')';
    }
}
