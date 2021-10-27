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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.MetadataRecordType;

public enum MetadataVersions implements MetadataVersion {
    UNSUPPORTED((short) -2, null, "Unsupported version, IBP < 3.1-IV1", false) {
        @Override
        public short recordVersion(MetadataRecordType type) {
            return type.lowestSupportedVersion();
        }
    },
    UNINITIALIZED((short) -1, null, "Uninitialized version", false) {
        @Override
        public short recordVersion(MetadataRecordType type) {
            return -1;
        }
    },
    V1((short) 1, null, "Initial version", false) {
        @Override
        public short recordVersion(MetadataRecordType type) {
            return type.lowestSupportedVersion();
        }
    },
    V2((short) 2, V1, "Second version", true) {
        @Override
        public short recordVersion(MetadataRecordType type) {
            if (type.equals(MetadataRecordType.FEATURE_LEVEL_RECORD)) {
                return 1;
            } else {
                return V1.recordVersion(type);
            }
        }
    };

    private final short version;
    private final MetadataVersion previous;
    private final String description;
    private final boolean isBackwardsCompatible;

    MetadataVersions(short version, MetadataVersion previous, String description, boolean isBackwardsCompatible) {
        this.version = version;
        this.previous = previous;
        this.description = description;
        this.isBackwardsCompatible = isBackwardsCompatible;
    }

    public static MetadataVersions of(short value) {
        for (MetadataVersions version : MetadataVersions.values()) {
            if (version.version == value) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unsupported metadata.version " + value + "!");
    }

    public static MetadataVersions latest() {
        return V2;
    }

    public static boolean isBackwardsCompatible(MetadataVersions sourceVersion, MetadataVersions targetVersion) {
        if (sourceVersion.compareTo(targetVersion) < 0) {
            return false;
        }
        MetadataVersion version = sourceVersion;
        while (version.isBackwardsCompatible() && version != targetVersion) {
            version = version.previous();
        }
        return version == targetVersion;
    }

    @Override
    public short version() {
        return version;
    }

    @Override
    public MetadataVersion previous() {
        return previous;
    }

    @Override
    public boolean isBackwardsCompatible() {
        return isBackwardsCompatible;
    }

    @Override
    public String description() {
        return description;
    }
}
