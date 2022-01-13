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

package org.apache.kafka.metadata;

import org.apache.kafka.common.metadata.MetadataRecordType;

import java.util.Optional;


public enum MetadataVersions implements MetadataVersion {
    UNINITIALIZED(0, null, "Uninitialized version", false, type -> {
        throw new IllegalStateException("Cannot determine record version with uninitialized metadata.version");
    }),
    V1(1, null, "KRaft preview version", false, MetadataVersions::recordResolverV1),
    V2(2, V1, "Initial KRaft version", true, MetadataVersions::recordResolverV2);

    private final short version;
    private final MetadataVersion previous;
    private final String description;
    private final boolean isBackwardsCompatible;
    private final MetadataRecordVersionResolver resolver;

    MetadataVersions(int version, MetadataVersion previous, String description, boolean isBackwardsCompatible, MetadataRecordVersionResolver resolver) {
        if (version > Short.MAX_VALUE || version < Short.MIN_VALUE) {
            throw new IllegalArgumentException("version must be a short");
        }
        this.version = (short) version;
        this.previous = previous;
        this.description = description;
        this.isBackwardsCompatible = isBackwardsCompatible;
        this.resolver = resolver;
    }

    public static MetadataVersions fromValue(short value) {
        for (MetadataVersions version : MetadataVersions.values()) {
            if (version.version == value) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unsupported metadata.version " + value + "!");
    }

    public static MetadataVersions stable() {
        return V2;
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
            Optional<MetadataVersion> prev = version.previous();
            if (prev.isPresent()) {
                version = prev.get();
            } else {
                break;
            }
        }
        return version == targetVersion;
    }


    @Override
    public short version() {
        return version;
    }

    @Override
    public Optional<MetadataVersion> previous() {
        return Optional.ofNullable(previous);
    }

    @Override
    public boolean isBackwardsCompatible() {
        return isBackwardsCompatible;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public short recordVersion(MetadataRecordType type) {
        return resolver.recordVersion(type);
    }

    private static short recordResolverV1(MetadataRecordType type) {
        return type.lowestSupportedVersion();
    }

    private static short recordResolverV2(MetadataRecordType type) {
        return type.highestSupportedVersion();
    }
}
