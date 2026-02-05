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

/**
 * Represents the finalized feature levels for a Kafka cluster.
 * <p>
 * This class can be in one of three states:
 * <ul>
 *   <li>Unknown - metadata version is not yet known (use {@link #unknown()})</li>
 *   <li>KRaft version only - only metadata version is known (use {@link #fromKRaftVersion(MetadataVersion)})</li>
 *   <li>Full features - metadata version, features map, and epoch are all known (use {@link #of(MetadataVersion, Map, long)})</li>
 * </ul>
 */
public final class FinalizedFeatures {
    private static final FinalizedFeatures UNKNOWN = new FinalizedFeatures(null, Map.of(), -1);

    private final MetadataVersion metadataVersion;
    private final Map<String, Short> finalizedFeatures;
    private final long finalizedFeaturesEpoch;

    private FinalizedFeatures(
        MetadataVersion metadataVersion,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch
    ) {
        this.metadataVersion = metadataVersion;
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        if (metadataVersion != null) {
            this.finalizedFeatures.put(MetadataVersion.FEATURE_NAME, metadataVersion.featureLevel());
        }
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
    public static FinalizedFeatures fromKRaftVersion(MetadataVersion version) {
        Objects.requireNonNull(version, "version cannot be null");
        return new FinalizedFeatures(version, Map.of(), -1);
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
        return new FinalizedFeatures(metadataVersion, finalizedFeatures, epoch);
    }

    /**
     * Returns whether the metadata version is known.
     *
     * @return true if the metadata version is known, false otherwise
     */
    public boolean isMetadataKnown() {
        return metadataVersion != null;
    }

    /**
     * Returns the metadata version, throwing an exception if unknown.
     *
     * @return the metadata version
     * @throws IllegalStateException if the metadata version is unknown
     */
    public MetadataVersion metadataVersionOrThrow() {
        if (metadataVersion == null) {
            throw new IllegalStateException("Metadata version is unknown");
        }
        return metadataVersion;
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
        if (metadataVersion == null) {
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
