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

import static org.apache.kafka.server.common.MetadataVersion.FEATURE_NAME;

public final class Features {
    private final MetadataVersion version;
    private final Map<String, Short> finalizedFeatures;
    private final long finalizedFeaturesEpoch;

    public static Features fromKRaftVersion(MetadataVersion version) {
        return new Features(version, Collections.emptyMap(), -1, true);
    }

    public Features(
        MetadataVersion version,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch,
        boolean kraftMode
    ) {
        this.version = version;
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        // In KRaft mode, we always include the metadata version in the features map.
        // In ZK mode, we never include it.
        if (kraftMode) {
            this.finalizedFeatures.put(FEATURE_NAME, version.featureLevel());
        } else {
            this.finalizedFeatures.remove(FEATURE_NAME);
        }
    }

    public MetadataVersion metadataVersion() {
        return version;
    }

    public Map<String, Short> finalizedFeatures() {
        return finalizedFeatures;
    }

    public long finalizedFeaturesEpoch() {
        return finalizedFeaturesEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(Features.class))) return false;
        Features other = (Features) o;
        return version == other.version &&
            finalizedFeatures.equals(other.finalizedFeatures) &&
                finalizedFeaturesEpoch == other.finalizedFeaturesEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, finalizedFeatures, finalizedFeaturesEpoch);
    }

    @Override
    public String toString() {
        return "Features" +
                "(version=" + version +
                ", finalizedFeatures=" + finalizedFeatures +
                ", finalizedFeaturesEpoch=" + finalizedFeaturesEpoch +
                ")";
    }
}
