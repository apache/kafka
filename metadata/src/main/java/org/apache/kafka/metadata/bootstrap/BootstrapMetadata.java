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

package org.apache.kafka.metadata.bootstrap;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;


/**
 * The bootstrap metadata. On startup, if the metadata log is empty, we will populate the log with
 * these records. Alternately, if log is not empty, but the metadata version is not set, we will
 * use the version specified here.
 */
public class BootstrapMetadata {
    private final List<ApiMessageAndVersion> records;
    private final MetadataVersion metadataVersion;
    private final String source;

    public static BootstrapMetadata fromVersion(MetadataVersion metadataVersion, String source) {
        List<ApiMessageAndVersion> records = Collections.singletonList(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()), (short) 0));
        return new BootstrapMetadata(records, metadataVersion, source);
    }

    public static BootstrapMetadata fromRecords(List<ApiMessageAndVersion> records, String source) {
        MetadataVersion metadataVersion = null;
        for (ApiMessageAndVersion record : records) {
            Optional<MetadataVersion> version = recordToMetadataVersion(record.message());
            if (version.isPresent()) {
                metadataVersion = version.get();
            }
        }
        if (metadataVersion == null) {
            throw new RuntimeException("No FeatureLevelRecord for " + MetadataVersion.FEATURE_NAME +
                    " was found in the bootstrap metadata from " + source);
        }
        return new BootstrapMetadata(records, metadataVersion, source);
    }

    public static Optional<MetadataVersion> recordToMetadataVersion(ApiMessage record) {
        if (record instanceof FeatureLevelRecord) {
            FeatureLevelRecord featureLevel = (FeatureLevelRecord) record;
            if (featureLevel.name().equals(MetadataVersion.FEATURE_NAME)) {
                return Optional.of(MetadataVersion.fromFeatureLevel(featureLevel.featureLevel()));
            }
        }
        return Optional.empty();
    }

    BootstrapMetadata(
        List<ApiMessageAndVersion> records,
        MetadataVersion metadataVersion,
        String source
    ) {
        this.records = Objects.requireNonNull(records);
        if (metadataVersion.isLessThan(MINIMUM_BOOTSTRAP_VERSION)) {
            throw new RuntimeException("Bootstrap metadata versions before " +
                    MINIMUM_BOOTSTRAP_VERSION + " are not supported. Can't load metadata from " +
                    source);
        }
        this.metadataVersion = metadataVersion;
        Objects.requireNonNull(source);
        this.source = source;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }

    public String source() {
        return source;
    }

    public BootstrapMetadata copyWithOnlyVersion() {
        ApiMessageAndVersion versionRecord = null;
        for (ApiMessageAndVersion record : records) {
            if (recordToMetadataVersion(record.message()).isPresent()) {
                versionRecord = record;
            }
        }
        if (versionRecord == null) {
            throw new RuntimeException("No FeatureLevelRecord for " + MetadataVersion.FEATURE_NAME +
                    " was found in " + source);
        }
        return new BootstrapMetadata(Collections.singletonList(versionRecord),
                metadataVersion, source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, metadataVersion, source);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        BootstrapMetadata other = (BootstrapMetadata) o;
        return Objects.equals(records, other.records) &&
            metadataVersion.equals(other.metadataVersion) &&
            source.equals(other.source);
    }

    @Override
    public String toString() {
        return "BootstrapMetadata(records=" + records.toString() +
            ", metadataVersion=" + metadataVersion +
            ", source=" + source +
            ")";
    }
}
