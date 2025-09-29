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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class FinalizedFeatures {
    private final MetadataVersion metadataVersion;
    private final Map<String, Short> finalizedFeatures;
    private final long finalizedFeaturesEpoch;

    public static FinalizedFeatures fromKRaftVersion(MetadataVersion version) {
        return new FinalizedFeatures(version, Collections.emptyMap(), -1, true);
    }

    public FinalizedFeatures(
        MetadataVersion metadataVersion,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch,
        boolean kraftMode
    ) {
        this.metadataVersion = metadataVersion;
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        // In KRaft mode, we always include the metadata version in the features map.
        // In ZK mode, we never include it.
        if (kraftMode) {
            this.finalizedFeatures.put(MetadataVersion.FEATURE_NAME, metadataVersion.featureLevel());
        } else {
            this.finalizedFeatures.remove(MetadataVersion.FEATURE_NAME);
        }
    }

    // Internal constructor only for copying FinalizedFeatures.
    private FinalizedFeatures(
        MetadataVersion metadataVersion,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch
    ) {
        this.metadataVersion = metadataVersion;
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }

    public Map<String, Short> finalizedFeatures() {
        return finalizedFeatures;
    }

    public long finalizedFeaturesEpoch() {
        return finalizedFeaturesEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(FinalizedFeatures.class))) return false;
        FinalizedFeatures other = (FinalizedFeatures) o;
        return metadataVersion == other.metadataVersion &&
            finalizedFeatures.equals(other.finalizedFeatures) &&
                finalizedFeaturesEpoch == other.finalizedFeaturesEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataVersion, finalizedFeatures, finalizedFeaturesEpoch);
    }

    @Override
    public String toString() {
        return "Features" +
                "(metadataVersion=" + metadataVersion +
                ", finalizedFeatures=" + finalizedFeatures +
                ", finalizedFeaturesEpoch=" + finalizedFeaturesEpoch +
                ")";
    }

    public FinalizedFeatures setFinalizedLevel(String key, short level) {
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
}
