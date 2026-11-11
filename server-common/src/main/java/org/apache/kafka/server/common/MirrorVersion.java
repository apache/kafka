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

import java.util.Map;

public enum MirrorVersion implements FeatureVersion {
    // Version 0 keeps Cluster Mirroring disabled
    MV_0(0, MetadataVersion.MINIMUM_VERSION, Map.of()),

    // Version 1 enables cluster mirroring (KIP-1279).
    // It depends on IBP_4_5_IV1 for the LeaderEpoch field in PartitionChangeRecord.
    // If IBP_4_5_IV1 is promoted before MV_1 is production-ready, move the bootstrap
    // version to a later MetadataVersion, so that new clusters keep mirroring off by default.
    MV_1(1, MetadataVersion.IBP_4_5_IV1, Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_5_IV1.featureLevel()));

    public static final String FEATURE_NAME = "mirror.version";

    // Level 1 is an early access feature and is not production ready.
    public static final MirrorVersion LATEST_PRODUCTION = MV_0;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;
    private final Map<String, Short> dependencies;

    MirrorVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion,
        Map<String, Short> dependencies
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.dependencies = dependencies;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return dependencies;
    }

    public static boolean isEnabled(Map<String, Short> finalizedFeatures) {
        short featureLevel = finalizedFeatures.getOrDefault(FEATURE_NAME, (short) 0);
        return featureLevel >= MV_1.featureLevel;
    }

    public static MirrorVersion fromFeatureLevel(short version) {
        return switch (version) {
            case 0 -> MV_0;
            case 1 -> MV_1;
            default -> throw new RuntimeException("Unknown mirror feature level: " + (int) version);
        };
    }
}
