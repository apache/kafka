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
import java.util.Map;

public enum KRaftVersion implements FeatureVersion {
    // Version 0 is the initial version of KRaft.
    KRAFT_VERSION_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION),

    // Version 1 enables KIP-853.
    KRAFT_VERSION_1(1, MetadataVersion.IBP_3_9_IV0);

    public static final String FEATURE_NAME = "kraft.version";

    public static final KRaftVersion LATEST_PRODUCTION = KRAFT_VERSION_1;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;

    KRaftVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    public static KRaftVersion fromFeatureLevel(short version) {
        switch (version) {
            case 0:
                return KRAFT_VERSION_0;
            case 1:
                return KRAFT_VERSION_1;
            default:
                throw new RuntimeException("Unknown KRaft feature level: " + (int) version);
        }
    }

    public boolean isReconfigSupported() {
        return this != KRAFT_VERSION_0;
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
        if (this.featureLevel == 0) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(
                MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_9_IV0.featureLevel());
        }
    }

    public short quorumStateVersion() {
        switch (this) {
            case KRAFT_VERSION_0:
                return (short) 0;
            case KRAFT_VERSION_1:
                return (short) 1;
        }
        throw new IllegalStateException("Unsupported KRaft feature level: " + this);
    }

    public short kraftVersionRecordVersion() {
        switch (this) {
            case KRAFT_VERSION_1:
                return (short) 0;
        }
        throw new IllegalStateException("Unsupported KRaft feature level: " + this);
    }

    public short votersRecordVersion() {
        switch (this) {
            case KRAFT_VERSION_1:
                return (short) 0;
        }
        throw new IllegalStateException("Unsupported KRaft feature level: " + this);
    }
}
