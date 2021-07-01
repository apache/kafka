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
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


/**
 * Represents the feature levels in the metadata image.
 *
 * This class is thread-safe.
 */
public final class FeaturesImage {
    public static final FeaturesImage EMPTY = new FeaturesImage(Collections.emptyMap());

    private final Map<String, VersionRange> finalizedVersions;

    public FeaturesImage(Map<String, VersionRange> finalizedVersions) {
        this.finalizedVersions = Collections.unmodifiableMap(finalizedVersions);
    }

    public boolean isEmpty() {
        return finalizedVersions.isEmpty();
    }

    Map<String, VersionRange> finalizedVersions() {
        return finalizedVersions;
    }

    private Optional<VersionRange> finalizedVersion(String feature) {
        return Optional.ofNullable(finalizedVersions.get(feature));
    }

    public void write(Consumer<List<ApiMessageAndVersion>> out) {
        List<ApiMessageAndVersion> batch = new ArrayList<>();
        for (Entry<String, VersionRange> entry : finalizedVersions.entrySet()) {
            batch.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(entry.getKey()).
                setMinFeatureLevel(entry.getValue().min()).
                setMaxFeatureLevel(entry.getValue().max()),
                FEATURE_LEVEL_RECORD.highestSupportedVersion()));
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
        return finalizedVersions.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", "));
    }
}
