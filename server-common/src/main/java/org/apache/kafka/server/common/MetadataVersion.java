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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.FinalizedVersionRange;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.requests.ApiVersionsResponse;

public enum MetadataVersion {
    IBP_0_8_0(-1),
    IBP_0_8_1(-1),
    IBP_0_8_2(-1),
    IBP_0_9_0(-1),
    IBP_0_10_0_IV0(-1),
    IBP_0_10_0_IV1(-1),
    IBP_0_10_1_IV0(-1),
    IBP_0_10_1_IV1(-1),
    IBP_0_10_1_IV2(-1),
    IBP_0_10_2_IV0(-1),
    IBP_0_11_0_IV0(-1),
    IBP_0_11_0_IV1(-1),
    IBP_0_11_0_IV2(-1),
    IBP_1_0_IV0(-1),
    IBP_1_1_IV0(-1),
    IBP_2_0_IV0(-1),
    IBP_2_0_IV1(-1),
    IBP_2_1_IV0(-1),
    IBP_2_1_IV1(-1),
    IBP_2_1_IV2(-1),
    IBP_2_2_IV0(-1),
    IBP_2_2_IV1(-1),
    IBP_2_3_IV0(-1),
    IBP_2_3_IV1(-1),
    IBP_2_4_IV0(-1),
    IBP_2_4_IV1(-1),
    IBP_2_5_IV0(-1),
    IBP_2_6_IV0(-1),
    IBP_2_7_IV0(-1),
    IBP_2_7_IV1(-1),
    IBP_2_7_IV2(-1),
    IBP_2_8_IV0(-1),
    IBP_2_8_IV1(-1),
    // KRaft preview
    IBP_3_0_IV0(1),
    IBP_3_0_IV1(2),
    IBP_3_1_IV0(3),
    IBP_3_2_IV0(4),
    // KRaft GA
    IBP_3_3_IV0(5);

    private final Optional<Short> metadataVersion;

    MetadataVersion(int metadataVersion) {
        if (metadataVersion > 0) {
            this.metadataVersion = Optional.of((short) metadataVersion);
        } else {
            this.metadataVersion = Optional.empty();
        }
    }

    public boolean isAlterIsrSupported() {
        return this.isAtLeast(IBP_2_7_IV2);
    }

    public boolean isAllocateProducerIdsSupported() {
        return this.isAtLeast(IBP_3_0_IV0);
    }

    public boolean isTruncationOnFetchSupported() {
        return this.isAtLeast(IBP_2_7_IV1);
    }

    public boolean isOffsetForLeaderEpochSupported() {
        return this.isAtLeast(IBP_0_11_0_IV2);
    }

    public boolean isTopicIdsSupported() {
        return this.isAtLeast(IBP_2_8_IV0);
    }

    public boolean isFeatureVersioningSupported() {
        return this.isAtLeast(IBP_2_7_IV1);
    }

    public boolean isSaslInterBrokerHandshakeRequestEnabled() {
        return this.isAtLeast(IBP_0_10_0_IV1);
    }

    public RecordVersion recordVersion() {
        if (this.compareTo(IBP_0_9_0) <= 0) { // IBPs up to IBP_0_9_0 use Record Version V0
            return RecordVersion.V0;
        } else if (this.compareTo(IBP_0_10_2_IV0) <= 0) { // IBPs up to IBP_0_10_2_IV0 use V1
            return RecordVersion.V1;
        } else return RecordVersion.V2; // all greater IBPs use V2
    }

    private static final Map<String, MetadataVersion> IBP_VERSIONS;
    static {
        {
            IBP_VERSIONS = new HashMap<>();
            Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
            Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
            for (MetadataVersion version : MetadataVersion.values()) {
                Matcher matcher = versionPattern.matcher(version.name());
                if (matcher.find()) {
                    String withoutIV = matcher.group(1);
                    // remove any trailing underscores
                    if (withoutIV.endsWith("_")) {
                        withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
                    }
                    String shortVersion = withoutIV.replace("_", ".");

                    String normalizedVersion;
                    if (matcher.group(2) != null) {
                        normalizedVersion = String.format("%s-IV%s", shortVersion, matcher.group(2));
                    } else {
                        normalizedVersion = shortVersion;
                    }
                    maxInterVersion.compute(shortVersion, (__, currentVersion) -> {
                        if (currentVersion == null) {
                            return version;
                        } else if (version.compareTo(currentVersion) > 0) {
                            return version;
                        } else {
                            return currentVersion;
                        }
                    });
                    IBP_VERSIONS.put(normalizedVersion, version);
                } else {
                    throw new IllegalArgumentException("Metadata version: " + version.name() + " does not fit "
                            + "any of the accepted patterns.");
                }
            }
            IBP_VERSIONS.putAll(maxInterVersion);
        }
    }

    public String shortVersion() {
        Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)");
        Matcher matcher = versionPattern.matcher(this.name());
        if (matcher.find()) {
            String withoutIV = matcher.group(1);
            // remove any trailing underscores
            if (withoutIV.endsWith("_")) {
                withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
            }
            return withoutIV.replace("_", ".");
        } else {
            throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
                    + "the accepted pattern.");
        }
    }

    public String version() {
        if (this.compareTo(IBP_0_10_0_IV0) < 0) { // versions less than this do not have IV versions
            return shortVersion();
        } else {
            Pattern ivPattern = Pattern.compile("^IBP_[\\d_]+IV(\\d)");
            Matcher matcher = ivPattern.matcher(this.name());
            if (matcher.find()) {
                return String.format("%s-%s", shortVersion(), matcher.group(1));
            } else {
                throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
                        + "the accepted pattern.");
            }
        }
    }

    /**
     * Return an `ApiVersion` instance for `versionString`, which can be in a variety of formats (e.g. "0.8.0", "0.8.0.x",
     * "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `ApiVersion`.
     */
    public static MetadataVersion apply(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = (versionString.startsWith("0.")) ? 3 : 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }
        return Optional.ofNullable(IBP_VERSIONS.get(key)).orElseThrow(() ->
                new IllegalArgumentException("Version " + versionString + "is not a valid version")
        );
    }

    /**
     * Return the minimum `MetadataVersion` that supports `RecordVersion`.
     */
    public static MetadataVersion minSupportedFor(RecordVersion recordVersion) {
        switch (recordVersion) {
            case V0:
                return IBP_0_8_0;
            case V1:
                return IBP_0_10_0_IV0;
            case V2:
                return IBP_0_11_0_IV0;
            default:
                throw new IllegalArgumentException("Invalid message format version " + recordVersion);
        }
    }

    public static ApiVersionsResponse apiVersionsResponse(
        Integer throttleTimeMs,
        RecordVersion minRecordVersion,
        Features<SupportedVersionRange> latestSupportedFeatures,
        NodeApiVersions controllerApiVersions,
        ListenerType listenerType
    ) {
        return apiVersionsResponse(
            throttleTimeMs,
            minRecordVersion,
            latestSupportedFeatures,
            Features.emptyFinalizedFeatures(),
            ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            controllerApiVersions,
            listenerType
        );
    }

    public static ApiVersionsResponse apiVersionsResponse(
        Integer throttleTimeMs,
        RecordVersion minRecordVersion,
        Features<SupportedVersionRange> latestSupportedFeatures,
        Features<FinalizedVersionRange> finalizedFeatures,
        Long finalizedFeaturesEpoch,
        NodeApiVersions controllerApiVersions,
        ListenerType listenerType
    ) {
        ApiVersionCollection apiKeys;
        if (controllerApiVersions != null) {
            apiKeys = ApiVersionsResponse.intersectForwardableApis(
                    listenerType, minRecordVersion, controllerApiVersions.allSupportedApiVersions());
        } else {
            apiKeys = ApiVersionsResponse.filterApis(minRecordVersion, listenerType);
        }

        return ApiVersionsResponse.createApiVersionsResponse(
                throttleTimeMs,
                apiKeys,
                latestSupportedFeatures,
                finalizedFeatures,
                finalizedFeaturesEpoch
        );
    }

    public boolean isAtLeast(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    public static MetadataVersion latest() {
        MetadataVersion[] values = MetadataVersion.values();
        return values[values.length - 1];
    }

    public static MetadataVersion stable() {
        return MetadataVersion.IBP_3_3_IV0;
    }

    public Optional<Short> metadataVersion() {
        return metadataVersion;
    }
}
