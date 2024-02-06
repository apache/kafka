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
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * Represents changes to the cluster in the metadata image.
 */
public final class FeaturesDelta {
    private final FeaturesImage image;

    private final Map<String, Optional<Short>> changes = new HashMap<>();

    private MetadataVersion metadataVersionChange = null;

    private ZkMigrationState zkMigrationStateChange = null;

    public FeaturesDelta(FeaturesImage image) {
        this.image = image;
    }

    public Map<String, Optional<Short>> changes() {
        return changes;
    }

    public Optional<ZkMigrationState> getZkMigrationStateChange() {
        return Optional.ofNullable(zkMigrationStateChange);
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
            metadataVersionChange = MetadataVersion.fromFeatureLevel(record.featureLevel());
        } else {
            if (record.featureLevel() == 0) {
                changes.put(record.name(), Optional.empty());
            } else {
                changes.put(record.name(), Optional.of(record.featureLevel()));
            }
        }
    }

    public void replay(ZkMigrationStateRecord record) {
        this.zkMigrationStateChange = ZkMigrationState.of(record.zkMigrationState());
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

        final MetadataVersion metadataVersion;
        if (metadataVersionChange == null) {
            metadataVersion = image.metadataVersion();
        } else {
            metadataVersion = metadataVersionChange;
        }

        final ZkMigrationState zkMigrationState;
        if (zkMigrationStateChange == null) {
            zkMigrationState = image.zkMigrationState();
        } else {
            zkMigrationState = zkMigrationStateChange;
        }
        return new FeaturesImage(newFinalizedVersions, metadataVersion, zkMigrationState);
    }

    @Override
    public String toString() {
        return "FeaturesDelta(" +
            "changes=" + changes +
            ", metadataVersionChange=" + metadataVersionChange +
            ", zkMigrationStateChange=" + zkMigrationStateChange +
            ')';
    }
}
