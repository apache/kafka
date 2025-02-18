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
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * Represents changes to the cluster in the metadata image.
 */
public final class FeaturesDelta {
    private static final short MINIMUM_PERSISTED_FEATURE_LEVEL = 4;
    private final FeaturesImage image;

    private final Map<String, Optional<Short>> changes = new HashMap<>();

    private MetadataVersion metadataVersionChange = null;

    public FeaturesDelta(FeaturesImage image) {
        this.image = image;
    }

    public Map<String, Optional<Short>> changes() {
        return changes;
    }

    public Optional<MetadataVersion> metadataVersionChange() {
        return Optional.ofNullable(metadataVersionChange);
    }

    public void finishSnapshot() {
        for (String featureName : image.finalizedVersions().keySet()) {
            if (!changes.containsKey(featureName)) {
                changes.put(featureName, Optional.empty());
            }
        }
    }

    public void replay(FeatureLevelRecord record) {
        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            // Support for the `metadata.version` feature flag was added in IBP_3_3_IV0, so it's possible (but unlikely) that we read
            // records with a feature level that is no longer supported for clusters that used a pre-release version of 3.3.0.
            // We automatically fallback to IBP_3_3_IV3 in that case. We use explicit versions instead of `MINIMUM_VERSION` because
            // we want to force an explicit decision if we change `MetadataVersion.MINIMUM_VERSION` in the future.
            if (record.featureLevel() >= MINIMUM_PERSISTED_FEATURE_LEVEL && record.featureLevel() <= MetadataVersion.IBP_3_3_IV3.featureLevel())
                metadataVersionChange = MetadataVersion.IBP_3_3_IV3;
            else
                metadataVersionChange = MetadataVersion.fromFeatureLevel(record.featureLevel());
        } else {
            if (record.featureLevel() == 0) {
                changes.put(record.name(), Optional.empty());
            } else {
                changes.put(record.name(), Optional.of(record.featureLevel()));
            }
        }
    }

    public FeaturesImage apply() {
        Map<String, Short> newFinalizedVersions =
            new HashMap<>(image.finalizedVersions().size());
        for (Entry<String, Short> entry : image.finalizedVersions().entrySet()) {
            String name = entry.getKey();
            Optional<Short> change = changes.get(name);
            if (change == null) {
                newFinalizedVersions.put(name, entry.getValue());
            } else if (change.isPresent()) {
                newFinalizedVersions.put(name, change.get());
            }
        }
        for (Entry<String, Optional<Short>> entry : changes.entrySet()) {
            String name = entry.getKey();
            Optional<Short> change = entry.getValue();
            if (!newFinalizedVersions.containsKey(name)) {
                if (change.isPresent()) {
                    newFinalizedVersions.put(name, change.get());
                }
            }
        }

        final Optional<MetadataVersion> metadataVersion;
        if (metadataVersionChange == null) {
            metadataVersion = image.metadataVersion();
        } else {
            metadataVersion = Optional.of(metadataVersionChange);
        }

        return new FeaturesImage(newFinalizedVersions, metadataVersion);
    }

    @Override
    public String toString() {
        return "FeaturesDelta(" +
            "changes=" + changes +
            ", metadataVersionChange=" + metadataVersionChange +
            ')';
    }
}
