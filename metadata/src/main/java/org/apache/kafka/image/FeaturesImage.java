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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


/**
 * Represents the feature levels in the metadata image.
 *
 * This class is thread-safe.
 */
public final class FeaturesImage {
    public static final FeaturesImage EMPTY = new FeaturesImage(Collections.emptyMap(), MetadataVersion.UNINITIALIZED);

    private final Map<String, Short> finalizedVersions;

    private final MetadataVersion metadataVersion;

    public FeaturesImage(Map<String, Short> finalizedVersions, MetadataVersion metadataVersion) {
        this.finalizedVersions = Collections.unmodifiableMap(finalizedVersions);
        this.metadataVersion = metadataVersion;
    }

    public boolean isEmpty() {
        return finalizedVersions.isEmpty();
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }

    public Map<String, Short> finalizedVersions() {
        return finalizedVersions;
    }

    private Optional<Short> finalizedVersion(String feature) {
        return Optional.ofNullable(finalizedVersions.get(feature));
    }

    public void write(Consumer<List<ApiMessageAndVersion>> out) {
        List<ApiMessageAndVersion> batch = new ArrayList<>();
        // Write out the metadata.version record first, and then the rest of the finalized features
        if (!metadataVersion().equals(MetadataVersion.UNINITIALIZED)) {
            batch.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));
        }
        for (Entry<String, Short> entry : finalizedVersions.entrySet()) {
            if (entry.getKey().equals(MetadataVersion.FEATURE_NAME)) {
                continue;
            }
            batch.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(entry.getKey()).
                setFeatureLevel(entry.getValue()), FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        }
        out.accept(batch);
    }

    @Override
    public int hashCode() {
        return finalizedVersions.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FeaturesImage)) return false;
        FeaturesImage other = (FeaturesImage) o;
        return finalizedVersions.equals(other.finalizedVersions);
    }


    @Override
    public String toString() {
        return "FeaturesImage{" +
                "finalizedVersions=" + finalizedVersions +
                ", metadataVersion=" + metadataVersion +
                '}';
    }
}
