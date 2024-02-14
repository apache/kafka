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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class FeatureControlManagerTest {

    @SuppressWarnings("unchecked")
    private static Map<String, VersionRange> rangeMap(Object... args) {
        Map<String, VersionRange> result = new HashMap<>();
        for (int i = 0; i < args.length; i += 3) {
            String feature = (String) args[i];
            Number low = (Number) args[i + 1];
            Number high = (Number) args[i + 2];
            result.put(feature, VersionRange.of(low.shortValue(), high.shortValue()));
        }
        return result;
    }

    private static Map<String, Short> versionMap(Object... args) {
        Map<String, Short> result = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String feature = (String) args[i];
            Number ver = (Number) args[i + 1];
            result.put(feature, ver.shortValue());
        }
        return result;
    }

    public static QuorumFeatures features(Object... args) {
        Map<String, VersionRange> features = QuorumFeatures.defaultFeatureMap(true);
        features.putAll(rangeMap(args));
        return new QuorumFeatures(0, features, emptyList());
    }

    private static Map<String, Short> updateMap(Object... args) {
        Map<String, Short> result = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String feature = (String) args[i];
            Number ver = (Number) args[i + 1];
            result.put(feature, ver.shortValue());
        }
        return result;
    }

    @Test
    public void testUpdateFeatures() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features("foo", 1, 2)).
            setSnapshotRegistry(snapshotRegistry).
            setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
            build();
        snapshotRegistry.getOrCreateSnapshot(-1);
        assertEquals(new FinalizedControllerFeatures(Collections.singletonMap("metadata.version", (short) 4), -1),
            manager.finalizedFeatures(-1));
        assertEquals(ControllerResult.atomicOf(emptyList(), Collections.
                singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Invalid update version 3 for feature foo. Local controller 0 only supports versions 1-2"))),
            manager.updateFeatures(updateMap("foo", 3),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                false));
        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
                updateMap("foo", 2, "bar", 1), Collections.emptyMap(),
                false);
        Map<String, ApiError> expectedMap = new HashMap<>();
        expectedMap.put("foo", ApiError.NONE);
        expectedMap.put("bar", new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 1 for feature bar. Local controller 0 does not support this feature."));
        assertEquals(expectedMap, result.response());
        List<ApiMessageAndVersion> expectedMessages = new ArrayList<>();
        expectedMessages.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 2),
            (short) 0));
        assertEquals(expectedMessages, result.records());
    }

    @Test
    public void testReplay() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureLevelRecord record = new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 2);

        snapshotRegistry.getOrCreateSnapshot(-1);
        FeatureControlManager manager = new FeatureControlManager.Builder().
                setLogContext(logContext).
                setQuorumFeatures(features("foo", 1, 2)).
                setSnapshotRegistry(snapshotRegistry).
                setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
                build();
        manager.replay(record);
        snapshotRegistry.getOrCreateSnapshot(123);
        assertEquals(new FinalizedControllerFeatures(versionMap("metadata.version", 4, "foo", 2), 123),
            manager.finalizedFeatures(123));
    }

    static ClusterFeatureSupportDescriber createFakeClusterFeatureSupportDescriber(
        List<Map.Entry<Integer, Map<String, VersionRange>>> brokerRanges,
        List<Map.Entry<Integer, Map<String, VersionRange>>> controllerRanges
    ) {
        return new ClusterFeatureSupportDescriber() {
            @Override
            public Iterator<Map.Entry<Integer, Map<String, VersionRange>>> brokerSupported() {
                return brokerRanges.iterator();
            }

            @Override
            public Iterator<Map.Entry<Integer, Map<String, VersionRange>>> controllerSupported() {
                return controllerRanges.iterator();
            }
        };
    }

    @Test
    public void testUpdateFeaturesErrorCases() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setLogContext(logContext).
            setQuorumFeatures(features("foo", 1, 5, "bar", 0, 3)).
            setSnapshotRegistry(snapshotRegistry).
            setClusterFeatureSupportDescriber(createFakeClusterFeatureSupportDescriber(
                Arrays.asList(new SimpleImmutableEntry<>(5, Collections.singletonMap("bar", VersionRange.of(0, 3)))),
                Arrays.asList())).
            build();

        assertEquals(ControllerResult.atomicOf(emptyList(),
            Collections.singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 3 for feature foo. Broker 5 does not support this feature."))),
                    manager.updateFeatures(updateMap("foo", 3),
                        Collections.singletonMap("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                        false));

        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
            updateMap("bar", 3), Collections.emptyMap(), false);
        assertEquals(Collections.singletonMap("bar", ApiError.NONE), result.response());
        manager.replay((FeatureLevelRecord) result.records().get(0).message());
        snapshotRegistry.getOrCreateSnapshot(3);

        assertEquals(ControllerResult.atomicOf(emptyList(), Collections.
                singletonMap("bar", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Invalid update version 2 for feature bar. Can't downgrade the version of this feature " +
                    "without setting the upgrade type to either safe or unsafe downgrade."))),
            manager.updateFeatures(updateMap("bar", 2), Collections.emptyMap(), false));

        assertEquals(
            ControllerResult.atomicOf(
                Collections.singletonList(
                    new ApiMessageAndVersion(
                        new FeatureLevelRecord()
                            .setName("bar")
                            .setFeatureLevel((short) 2),
                        (short) 0
                    )
                ),
                Collections.singletonMap("bar", ApiError.NONE)
            ),
            manager.updateFeatures(
                updateMap("bar", 2),
                Collections.singletonMap("bar", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                false)
        );
    }

    @Test
    public void testReplayRecords() throws Exception {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setLogContext(logContext).
            setQuorumFeatures(features("foo", 1, 5, "bar", 1, 2)).
            setSnapshotRegistry(snapshotRegistry).
            setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
            build();
        ControllerResult<Map<String, ApiError>> result = manager.
            updateFeatures(updateMap("foo", 5, "bar", 1), Collections.emptyMap(), false);
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(MetadataVersion.IBP_3_3_IV0, manager.metadataVersion());
        assertEquals(Optional.of((short) 5), manager.finalizedFeatures(Long.MAX_VALUE).get("foo"));
        assertEquals(Optional.of((short) 1), manager.finalizedFeatures(Long.MAX_VALUE).get("bar"));
        assertEquals(new HashSet<>(Arrays.asList(
            MetadataVersion.FEATURE_NAME, "foo", "bar")),
                manager.finalizedFeatures(Long.MAX_VALUE).featureNames());
    }

    private static final FeatureControlManager.Builder TEST_MANAGER_BUILDER1 =
        new FeatureControlManager.Builder().
            setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.IBP_3_3_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV3.featureLevel())).
            setMetadataVersion(MetadataVersion.IBP_3_3_IV2);

    @Test
    public void testApplyMetadataVersionChangeRecord() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        manager.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_3_IV3.featureLevel()));
        assertEquals(MetadataVersion.IBP_3_3_IV3, manager.metadataVersion());
    }

    @Test
    public void testCannotDowngradeToVersionBeforeMinimumSupportedKraftVersion() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 3 for feature metadata.version. Local controller 0 only " +
                "supports versions 4-7"))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                true));
    }

    @Test
    public void testCannotDowngradeToHigherVersion() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 7 for feature metadata.version. Can't downgrade to a " +
                "newer version."))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                true));
    }

    @Test
    public void testCannotUnsafeDowngradeToHigherVersion() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 7 for feature metadata.version. Can't downgrade to a " +
                "newer version."))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                true));
    }

    @Test
    public void testCannotUpgradeToLowerVersion() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 4 for feature metadata.version. Can't downgrade the " +
                "version of this feature without setting the upgrade type to either safe or " +
                "unsafe downgrade."))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
                true));
    }

    @Test
    public void testCanUpgradeToHigherVersion() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, ApiError.NONE)),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
                true));
    }

    @Test
    public void testCannotUseSafeDowngradeIfMetadataChanged() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid metadata.version 4. Refusing to perform the requested downgrade because " +
                "it might delete metadata information."))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                true));
    }

    @Test
    public void testUnsafeDowngradeIsTemporarilyDisabled() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
                        singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                                "Invalid metadata.version 4. Unsafe metadata downgrade is not supported in this version."))),
                manager.updateFeatures(
                        singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel()),
                        singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                        true));
    }

    @Disabled
    @Test
    public void testCanUseUnsafeDowngradeIfMetadataChanged() {
        FeatureControlManager manager = TEST_MANAGER_BUILDER1.build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
                        singletonMap(MetadataVersion.FEATURE_NAME, ApiError.NONE)),
                manager.updateFeatures(
                        singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel()),
                        singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                        true));
    }

    @Test
    public void testCanUseSafeDowngradeIfMetadataDidNotChange() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
                setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                        MetadataVersion.IBP_3_0_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV1.featureLevel())).
                setMetadataVersion(MetadataVersion.IBP_3_1_IV0).
                setMinimumBootstrapVersion(MetadataVersion.IBP_3_0_IV0).
                build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
                        singletonMap(MetadataVersion.FEATURE_NAME, ApiError.NONE)),
                manager.updateFeatures(
                        singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_0_IV1.featureLevel()),
                        singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                        true));
    }

    @Test
    public void testCannotDowngradeBefore3_3_IV0() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.IBP_3_0_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV3.featureLevel())).
            setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
            build();
        assertEquals(ControllerResult.of(Collections.emptyList(),
                        singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                        "Invalid metadata.version 3. Unable to set a metadata.version less than 3.3-IV0"))),
                manager.updateFeatures(
                        singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel()),
                        singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                        true));
    }

    @Test
    public void testCreateFeatureLevelRecords() {
        Map<String, VersionRange> localSupportedFeatures = new HashMap<>();
        localSupportedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
            MetadataVersion.IBP_3_0_IV1.featureLevel(), MetadataVersion.latestTesting().featureLevel()));
        localSupportedFeatures.put("foo", VersionRange.of(0, 2));
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0, localSupportedFeatures, emptyList())).
            setClusterFeatureSupportDescriber(createFakeClusterFeatureSupportDescriber(
                Arrays.asList(new SimpleImmutableEntry<>(1, Collections.singletonMap("foo", VersionRange.of(0, 3)))),
                Arrays.asList())).
                build();
        ControllerResult<Map<String, ApiError>> result  = manager.updateFeatures(
                Collections.singletonMap("foo", (short) 1),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.UPGRADE),
                false);
        assertEquals(ControllerResult.atomicOf(Arrays.asList(new ApiMessageAndVersion(
                new FeatureLevelRecord().setName("foo").setFeatureLevel((short) 1), (short) 0)),
                        Collections.singletonMap("foo", ApiError.NONE)), result);
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(Optional.of((short) 1), manager.finalizedFeatures(Long.MAX_VALUE).get("foo"));

        ControllerResult<Map<String, ApiError>> result2  = manager.updateFeatures(
                Collections.singletonMap("foo", (short) 0),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                false);
        assertEquals(ControllerResult.atomicOf(Arrays.asList(new ApiMessageAndVersion(
                        new FeatureLevelRecord().setName("foo").setFeatureLevel((short) 0), (short) 0)),
                Collections.singletonMap("foo", ApiError.NONE)), result2);
        RecordTestUtils.replayAll(manager, result2.records());
        assertEquals(Optional.empty(), manager.finalizedFeatures(Long.MAX_VALUE).get("foo"));
    }

    @Test
    public void testNoMetadataVersionChangeDuringMigration() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                    MetadataVersion.IBP_3_0_IV0.featureLevel(), MetadataVersion.IBP_3_5_IV1.featureLevel())).
            setMetadataVersion(MetadataVersion.IBP_3_4_IV0).
            build();
        BootstrapMetadata bootstrapMetadata = BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_4_IV0, "FeatureControlManagerTest");
        RecordTestUtils.replayAll(manager, bootstrapMetadata.records());
        RecordTestUtils.replayOne(manager, ZkMigrationState.PRE_MIGRATION.toRecord());

        assertEquals(ControllerResult.of(Collections.emptyList(),
            singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid metadata.version 10. Unable to modify metadata.version while a ZK migration is in progress."))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_5_IV1.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
                true));

        assertEquals(ControllerResult.of(Collections.emptyList(),
                singletonMap(MetadataVersion.FEATURE_NAME, new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid metadata.version 4. Unable to modify metadata.version while a ZK migration is in progress."))),
            manager.updateFeatures(
                singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel()),
                singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                true));

        // Complete the migration
        RecordTestUtils.replayOne(manager, ZkMigrationState.POST_MIGRATION.toRecord());
        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
            singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_5_IV1.featureLevel()),
            singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
            false);
        assertEquals(Errors.NONE, result.response().get(MetadataVersion.FEATURE_NAME).error());
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(MetadataVersion.IBP_3_5_IV1, manager.metadataVersion());
    }
}
