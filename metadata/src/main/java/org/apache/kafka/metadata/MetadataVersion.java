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


/**
 * An enumeration of the valid metadata versions for the cluster.
 */
public enum MetadataVersion {

    UNINITIALIZED(0, null, "Uninitialized version", false, type -> {
        throw new IllegalStateException("Cannot determine record version with uninitialized metadata.version");
    }),
    V1(1, null, "KRaft preview version", false, MetadataVersion::recordResolverV1),
    V2(2, V1, "Initial KRaft version", true, MetadataVersion::recordResolverV2);


    public static final String FEATURE_NAME = "metadata.version";

    private final short version;
    private final MetadataVersion previous;
    private final String description;
    private final boolean isBackwardsCompatible;
    private final MetadataRecordVersionResolver resolver;

    MetadataVersion(int version, MetadataVersion previous, String description, boolean isBackwardsCompatible, MetadataRecordVersionResolver resolver) {
        if (version > Short.MAX_VALUE || version < Short.MIN_VALUE) {
            throw new IllegalArgumentException("version must be a short");
        }
        this.version = (short) version;
        this.previous = previous;
        this.description = description;
        this.isBackwardsCompatible = isBackwardsCompatible;
        this.resolver = resolver;
    }

    public static MetadataVersion fromValue(short value) {
        for (MetadataVersion version : MetadataVersion.values()) {
            if (version.version == value) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unsupported metadata.version " + value + "!");
    }

    public static MetadataVersion stable() {
        return V2;
    }

    public static MetadataVersion latest() {
        return V2;
    }

    public static boolean isBackwardsCompatible(MetadataVersion sourceVersion, MetadataVersion targetVersion) {
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


    public short version() {
        return version;
    }

    public Optional<MetadataVersion> previous() {
        return Optional.ofNullable(previous);
    }

    public boolean isBackwardsCompatible() {
        return isBackwardsCompatible;
    }

    public String description() {
        return description;
    }

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
