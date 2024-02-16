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
import java.util.regex.Pattern;
import org.apache.kafka.common.record.RecordVersion;

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate between themselves.
 * This is only for inter-broker communications - when communicating with clients, the client decides on the API version.
 * <br>
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another if it is lower in the enum list (to avoid depending on lexicographic order)
 * <br>
 * Since the api protocol may change more than once within the same release and to facilitate people deploying code from
 * trunk, we have the concept of internal versions (first introduced during the 0.10.0 development cycle). For example,
 * the first time we introduce a version change in a release, say 0.10.0, we will add a config value "0.10.0-IV0" and a
 * corresponding enum constant IBP_0_10_0-IV0. We will also add a config value "0.10.0" that will be mapped to the
 * latest internal version object, which is IBP_0_10_0-IV0. When we change the protocol a second time while developing
 * 0.10.0, we will add a new config value "0.10.0-IV1" and a corresponding enum constant IBP_0_10_0-IV1. We will change
 * the config value "0.10.0" to map to the latest internal version IBP_0_10_0-IV1. The config value of
 * "0.10.0-IV0" is still mapped to IBP_0_10_0-IV0. This way, if people are deploying from trunk, they can use
 * "0.10.0-IV0" and "0.10.0-IV1" to upgrade one internal version at a time. For most people who just want to use
 * released version, they can use "0.10.0" when upgrading to the 0.10.0 release.
 */
public enum MetadataVersion {

    IBP_0_8_0(-1, "0.8.0", ""),
    IBP_0_8_1(-1, "0.8.1", ""),
    IBP_0_8_2(-1, "0.8.2", ""),
    IBP_0_9_0(-1, "0.9.0", ""),

    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    IBP_0_10_0_IV0(-1, "0.10.0", "IV0"),

    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    IBP_0_10_0_IV1(-1, "0.10.0", "IV1"),

    // introduced for JoinGroup protocol change in KIP-62
    IBP_0_10_1_IV0(-1, "0.10.1", "IV0"),

    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    IBP_0_10_1_IV1(-1, "0.10.1", "IV1"),

    // introduced ListOffsetRequest v1 in KIP-79
    IBP_0_10_1_IV2(-1, "0.10.1", "IV2"),

    // introduced UpdateMetadataRequest v3 in KIP-103
    IBP_0_10_2_IV0(-1, "0.10.2", "IV0"),

    // KIP-98 (idempotent and transactional producer support)
    IBP_0_11_0_IV0(-1, "0.11.0", "IV0"),

    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    IBP_0_11_0_IV1(-1, "0.11.0", "IV1"),

    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    IBP_0_11_0_IV2(-1, "0.11.0", "IV2"),

    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    IBP_1_0_IV0(-1, "1.0", "IV0"),

    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    IBP_1_1_IV0(-1, "1.1", "IV0"),

    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279 (Fix log divergence between leader and follower after fast leader fail over)
    IBP_2_0_IV0(-1, "2.0", "IV0"),

    // Several request versions were bumped due to KIP-219 (Improve quota communication)
    IBP_2_0_IV1(-1, "2.0", "IV1"),

    // Introduced new schemas for group offset (v2) and group metadata (v2) (KIP-211)
    IBP_2_1_IV0(-1, "2.1", "IV0"),

    // New Fetch, OffsetsForLeaderEpoch, and ListOffsets schemas (KIP-320)
    IBP_2_1_IV1(-1, "2.1", "IV1"),

    // Support ZStandard Compression Codec (KIP-110)
    IBP_2_1_IV2(-1, "2.1", "IV2"),

    // Introduced broker generation (KIP-380), and
    // LeaderAndIsrRequest V2, UpdateMetadataRequest V5, StopReplicaRequest V1
    IBP_2_2_IV0(-1, "2.2", "IV0"),

    // New error code for ListOffsets when a new leader is lagging behind former HW (KIP-207)
    IBP_2_2_IV1(-1, "2.2", "IV1"),

    // Introduced static membership.
    IBP_2_3_IV0(-1, "2.3", "IV0"),

    // Add rack_id to FetchRequest, preferred_read_replica to FetchResponse, and replica_id to OffsetsForLeaderRequest
    IBP_2_3_IV1(-1, "2.3", "IV1"),

    // Add adding_replicas and removing_replicas fields to LeaderAndIsrRequest
    IBP_2_4_IV0(-1, "2.4", "IV0"),

    // Flexible version support in inter-broker APIs
    IBP_2_4_IV1(-1, "2.4", "IV1"),

    // No new APIs, equivalent to 2.4-IV1
    IBP_2_5_IV0(-1, "2.5", "IV0"),

    // Introduced StopReplicaRequest V3 containing the leader epoch for each partition (KIP-570)
    IBP_2_6_IV0(-1, "2.6", "IV0"),

    // Introduced feature versioning support (KIP-584)
    IBP_2_7_IV0(-1, "2.7", "IV0"),

    // Bup Fetch protocol for Raft protocol (KIP-595)
    IBP_2_7_IV1(-1, "2.7", "IV1"),

    // Introduced AlterPartition (KIP-497)
    IBP_2_7_IV2(-1, "2.7", "IV2"),

    // Flexible versioning on ListOffsets, WriteTxnMarkers and OffsetsForLeaderEpoch. Also adds topic IDs (KIP-516)
    IBP_2_8_IV0(-1, "2.8", "IV0"),

    // Introduced topic IDs to LeaderAndIsr and UpdateMetadata requests/responses (KIP-516)
    IBP_2_8_IV1(-1, "2.8", "IV1"),

    // Introduce AllocateProducerIds (KIP-730)
    IBP_3_0_IV0(-1, "3.0", "IV0"),

    // Introduce ListOffsets V7 which supports listing offsets by max timestamp (KIP-734)
    // Assume message format version is 3.0 (KIP-724)
    IBP_3_0_IV1(1, "3.0", "IV1", true),

    // Adds topic IDs to Fetch requests/responses (KIP-516)
    IBP_3_1_IV0(2, "3.1", "IV0", false),

    // Support for leader recovery for unclean leader election (KIP-704)
    IBP_3_2_IV0(3, "3.2", "IV0", true),

    // Support for metadata.version feature flag and Removes min_version_level from the finalized version range that is written to ZooKeeper (KIP-778)
    IBP_3_3_IV0(4, "3.3", "IV0", false),

    // Support NoopRecord for the cluster metadata log (KIP-835)
    IBP_3_3_IV1(5, "3.3", "IV1", true),

    // In KRaft mode, use BrokerRegistrationChangeRecord instead of UnfenceBrokerRecord and FenceBrokerRecord.
    IBP_3_3_IV2(6, "3.3", "IV2", true),

    // Adds InControlledShutdown state to RegisterBrokerRecord and BrokerRegistrationChangeRecord (KIP-841).
    IBP_3_3_IV3(7, "3.3", "IV3", true),

    // Adds ZK to KRaft migration support (KIP-866). This includes ZkMigrationRecord, a new version of RegisterBrokerRecord,
    // and updates to a handful of RPCs.
    IBP_3_4_IV0(8, "3.4", "IV0", true),

    // Support for tiered storage (KIP-405)
    IBP_3_5_IV0(9, "3.5", "IV0", false),

    // Adds replica epoch to Fetch request (KIP-903).
    IBP_3_5_IV1(10, "3.5", "IV1", false),

    // KRaft support for SCRAM
    IBP_3_5_IV2(11, "3.5", "IV2", true),

    // Remove leader epoch bump when KRaft controller shrinks the ISR (KAFKA-15021)
    IBP_3_6_IV0(12, "3.6", "IV0", false),

    // Add metadata transactions
    IBP_3_6_IV1(13, "3.6", "IV1", true),

    // Add KRaft support for Delegation Tokens
    IBP_3_6_IV2(14, "3.6", "IV2", true),

    // Implement KIP-919 controller registration.
    IBP_3_7_IV0(15, "3.7", "IV0", true),

    // Reserved
    IBP_3_7_IV1(16, "3.7", "IV1", false),

    // Add JBOD support for KRaft.
    IBP_3_7_IV2(17, "3.7", "IV2", true),

    // IBP_3_7_IV3 was reserved for ELR support (KIP-966) but has been moved forward to
    // a later release requiring a new MetadataVersion. MVs are not reused.
    IBP_3_7_IV3(18, "3.7", "IV3", false),

    // Add new fetch request version for KIP-951
    IBP_3_7_IV4(19, "3.7", "IV4", false),

    // Add ELR related supports (KIP-966).
    IBP_3_8_IV0(20, "3.8", "IV0", true);

    // NOTES when adding a new version:
    //   Update the default version in @ClusterTest annotation to point to the latest version
    //   Change expected message in org.apache.kafka.tools.FeatureCommandTest in multiple places (search for "Change expected message")
    public static final String FEATURE_NAME = "metadata.version";

    /**
     * The first version we currently support in KRaft.
     */
    public static final MetadataVersion MINIMUM_KRAFT_VERSION = IBP_3_0_IV1;

    /**
     * The first version we currently support in the bootstrap metadata. We chose 3.3IV0 since it
     * is the first version that supports storing the metadata.version in the log.
     */
    public static final MetadataVersion MINIMUM_BOOTSTRAP_VERSION = IBP_3_3_IV0;

    /**
     * The latest production-ready MetadataVersion. This is the latest version that is stable
     * and cannot be changed. MetadataVersions later than this can be tested via junit, but
     * not deployed in production.
     *
     * <strong>Think carefully before you update this value. ONCE A METADATA VERSION IS PRODUCTION,
     * IT CANNOT BE CHANGED.</strong>
     */
    public static final MetadataVersion LATEST_PRODUCTION = IBP_3_7_IV4;

    /**
     * An array containing all of the MetadataVersion entries.
     *
     * This is essentially a cached copy of MetadataVersion.values. Unlike that function, it doesn't
     * allocate a new array each time.
     */
    public static final MetadataVersion[] VERSIONS;

    private final short featureLevel;
    private final String release;
    private final String ibpVersion;
    private final boolean didMetadataChange;

    MetadataVersion(int featureLevel, String release, String subVersion) {
        this(featureLevel, release, subVersion, true);
    }

    MetadataVersion(int featureLevel, String release, String subVersion, boolean didMetadataChange) {
        this.featureLevel = (short) featureLevel;
        this.release = release;
        if (subVersion.isEmpty()) {
            this.ibpVersion = release;
        } else {
            this.ibpVersion = String.format("%s-%s", release, subVersion);
        }
        this.didMetadataChange = didMetadataChange;
    }

    public short featureLevel() {
        return featureLevel;
    }

    public boolean isSaslInterBrokerHandshakeRequestEnabled() {
        return this.isAtLeast(IBP_0_10_0_IV1);
    }

    public boolean isOffsetForLeaderEpochSupported() {
        return this.isAtLeast(IBP_0_11_0_IV2);
    }

    public boolean isFeatureVersioningSupported() {
        return this.isAtLeast(IBP_2_7_IV0);
    }

    public boolean isTruncationOnFetchSupported() {
        return this.isAtLeast(IBP_2_7_IV1);
    }

    public boolean isAlterPartitionSupported() {
        return this.isAtLeast(IBP_2_7_IV2);
    }

    public boolean isTopicIdsSupported() {
        return this.isAtLeast(IBP_2_8_IV0);
    }

    public boolean isAllocateProducerIdsSupported() {
        return this.isAtLeast(IBP_3_0_IV0);
    }

    public boolean isLeaderRecoverySupported() {
        return this.isAtLeast(IBP_3_2_IV0);
    }

    public boolean isNoOpRecordSupported() {
        return this.isAtLeast(IBP_3_3_IV1);
    }

    public boolean isApiForwardingEnabled() {
        return this.isAtLeast(IBP_3_4_IV0);
    }

    public boolean isScramSupported() {
        return this.isAtLeast(IBP_3_5_IV2);
    }

    public boolean isLeaderEpochBumpRequiredOnIsrShrink() {
        return !this.isAtLeast(IBP_3_6_IV0);
    }

    public boolean isMetadataTransactionSupported() {
        return this.isAtLeast(IBP_3_6_IV1);
    }

    public boolean isDelegationTokenSupported() {
        return this.isAtLeast(IBP_3_6_IV2);
    }

    public boolean isDirectoryAssignmentSupported() {
        return this.isAtLeast(IBP_3_7_IV2);
    }

    public boolean isElrSupported() {
        return this.isAtLeast(IBP_3_8_IV0);
    }

    public boolean isKRaftSupported() {
        return this.featureLevel > 0;
    }

    public RecordVersion highestSupportedRecordVersion() {
        if (this.isLessThan(IBP_0_10_0_IV0)) {
            return RecordVersion.V0;
        } else if (this.isLessThan(IBP_0_11_0_IV0)) {
            return RecordVersion.V1;
        } else {
            return RecordVersion.V2;
        }
    }

    public boolean isBrokerRegistrationChangeRecordSupported() {
        return this.isAtLeast(IBP_3_3_IV2);
    }

    public boolean isInControlledShutdownStateSupported() {
        return this.isAtLeast(IBP_3_3_IV3);
    }

    public boolean isMigrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_3_4_IV0);
    }

    public short registerBrokerRecordVersion() {
        if (isDirectoryAssignmentSupported()) {
            // new logDirs field
            return (short) 3;
        } else if (isMigrationSupported()) {
            // new isMigrationZkBroker field
            return (short) 2;
        } else if (isInControlledShutdownStateSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short registerControllerRecordVersion() {
        if (isAtLeast(MetadataVersion.IBP_3_7_IV0)) {
            return (short) 0;
        } else {
            throw new RuntimeException("Controller registration is not supported in " +
                    "MetadataVersion " + this);
        }
    }

    public boolean isControllerRegistrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_3_7_IV0);
    }

    public short partitionChangeRecordVersion() {
        if (isElrSupported()) {
            return (short) 2;
        } else if (isDirectoryAssignmentSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short partitionRecordVersion() {
        if (isElrSupported()) {
            return (short) 2;
        } else if (isDirectoryAssignmentSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short fetchRequestVersion() {
        if (this.isAtLeast(IBP_3_7_IV4)) {
            return 16;
        } else if (this.isAtLeast(IBP_3_5_IV1)) {
            return 15;
        } else if (this.isAtLeast(IBP_3_5_IV0)) {
            return 14;
        } else if (this.isAtLeast(IBP_3_1_IV0)) {
            return 13;
        } else if (this.isAtLeast(IBP_2_7_IV1)) {
            return 12;
        } else if (this.isAtLeast(IBP_2_3_IV1)) {
            return 11;
        } else if (this.isAtLeast(IBP_2_1_IV2)) {
            return 10;
        } else if (this.isAtLeast(IBP_2_0_IV1)) {
            return 8;
        } else if (this.isAtLeast(IBP_1_1_IV0)) {
            return 7;
        } else if (this.isAtLeast(IBP_0_11_0_IV1)) {
            return 5;
        } else if (this.isAtLeast(IBP_0_11_0_IV0)) {
            return 4;
        } else if (this.isAtLeast(IBP_0_10_1_IV1)) {
            return 3;
        } else if (this.isAtLeast(IBP_0_10_0_IV0)) {
            return 2;
        } else if (this.isAtLeast(IBP_0_9_0)) {
            return 1;
        } else {
            return 0;
        }
    }

    public short offsetForLeaderEpochRequestVersion() {
        if (this.isAtLeast(IBP_2_8_IV0)) {
            return 4;
        } else if (this.isAtLeast(IBP_2_3_IV1)) {
            return 3;
        } else if (this.isAtLeast(IBP_2_1_IV1)) {
            return 2;
        } else if (this.isAtLeast(IBP_2_0_IV0)) {
            return 1;
        } else {
            return 0;
        }
    }

    public short listOffsetRequestVersion() {
        if (this.isAtLeast(IBP_3_5_IV0)) {
            return 8;
        } else if (this.isAtLeast(IBP_3_0_IV1)) {
            return 7;
        } else if (this.isAtLeast(IBP_2_8_IV0)) {
            return 6;
        } else if (this.isAtLeast(IBP_2_2_IV1)) {
            return 5;
        } else if (this.isAtLeast(IBP_2_1_IV1)) {
            return 4;
        } else if (this.isAtLeast(IBP_2_0_IV1)) {
            return 3;
        } else if (this.isAtLeast(IBP_0_11_0_IV0)) {
            return 2;
        } else if (this.isAtLeast(IBP_0_10_1_IV2)) {
            return 1;
        } else {
            return 0;
        }
    }

    public short groupMetadataValueVersion() {
        if (this.isLessThan(IBP_0_10_1_IV0)) {
            return 0;
        } else if (this.isLessThan(IBP_2_1_IV0)) {
            return 1;
        } else if (this.isLessThan(IBP_2_3_IV0)) {
            return 2;
        } else {
            // Serialize with the highest supported non-flexible version
            // until a tagged field is introduced or the version is bumped.
            return 3;
        }
    }

    public short offsetCommitValueVersion(boolean expireTimestampMs) {
        if (isLessThan(MetadataVersion.IBP_2_1_IV0) || expireTimestampMs) {
            return 1;
        } else if (isLessThan(MetadataVersion.IBP_2_1_IV1)) {
            return 2;
        } else {
            // Serialize with the highest supported non-flexible version
            // until a tagged field is introduced or the version is bumped.
            return  3;
        }
    }

    private static final Map<String, MetadataVersion> IBP_VERSIONS;

    static {
        MetadataVersion[] enumValues = MetadataVersion.values();
        VERSIONS = Arrays.copyOf(enumValues, enumValues.length);

        IBP_VERSIONS = new HashMap<>();
        Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
        for (MetadataVersion metadataVersion : VERSIONS) {
            if (metadataVersion.isProduction()) {
                maxInterVersion.put(metadataVersion.release, metadataVersion);
            }
            IBP_VERSIONS.put(metadataVersion.ibpVersion, metadataVersion);
        }
        IBP_VERSIONS.putAll(maxInterVersion);
    }

    public boolean isProduction() {
        return this.compareTo(MetadataVersion.LATEST_PRODUCTION) <= 0;
    }

    public String shortVersion() {
        return release;
    }

    public String version() {
        return ibpVersion;
    }

    public boolean didMetadataChange() {
        return didMetadataChange;
    }

    Optional<MetadataVersion> previous() {
        int idx = this.ordinal();
        if (idx > 0) {
            return Optional.of(VERSIONS[idx - 1]);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Return an `MetadataVersion` instance for `versionString`, which can be in a variety of formats (e.g. "0.8.0", "0.8.0.x",
     * "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `MetadataVersion`.
     * Note that 'misconfigured' values such as "1.0.1" will be parsed to `IBP_1_0_IV0` as we ignore anything after the first
     * two digits for versions that don't start with "0."
     */
    public static MetadataVersion fromVersionString(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = (versionString.startsWith("0.")) ? 3 : 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }
        return Optional.ofNullable(IBP_VERSIONS.get(key)).orElseThrow(() ->
            new IllegalArgumentException("Version " + versionString + " is not a valid version")
        );
    }

    public static MetadataVersion fromFeatureLevel(short version) {
        for (MetadataVersion metadataVersion: MetadataVersion.values()) {
            if (metadataVersion.featureLevel() == version) {
                return metadataVersion;
            }
        }
        throw new IllegalArgumentException("No MetadataVersion with metadata version " + version);
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

    // Testing only
    public static MetadataVersion latestTesting() {
        return VERSIONS[VERSIONS.length - 1];
    }

    public static MetadataVersion latestProduction() {
        return LATEST_PRODUCTION;
    }

    public static boolean checkIfMetadataChanged(MetadataVersion sourceVersion, MetadataVersion targetVersion) {
        if (sourceVersion == targetVersion) {
            return false;
        }

        final MetadataVersion highVersion, lowVersion;
        if (sourceVersion.compareTo(targetVersion) < 0) {
            highVersion = targetVersion;
            lowVersion = sourceVersion;
        } else {
            highVersion = sourceVersion;
            lowVersion = targetVersion;
        }
        return checkIfMetadataChangedOrdered(highVersion, lowVersion);
    }

    private static boolean checkIfMetadataChangedOrdered(MetadataVersion highVersion, MetadataVersion lowVersion) {
        MetadataVersion version = highVersion;
        while (!version.didMetadataChange() && version != lowVersion) {
            Optional<MetadataVersion> prev = version.previous();
            if (prev.isPresent()) {
                version = prev.get();
            } else {
                break;
            }
        }
        return version != lowVersion;
    }

    public boolean isAtLeast(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    @Override
    public String toString() {
        return ibpVersion;
    }
}
