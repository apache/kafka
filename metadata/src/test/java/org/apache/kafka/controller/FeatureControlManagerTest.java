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

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionTestUtils;
import org.apache.kafka.server.common.TestFeatureVersion;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class FeatureControlManagerTest {

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
        Map<String, VersionRange> features = QuorumFeatures.defaultSupportedFeatureMap(true);
        features.putAll(rangeMap(args));
        return new QuorumFeatures(0, features, List.of());
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
            setQuorumFeatures(features(TestFeatureVersion.FEATURE_NAME, 0, 2)).
            setSnapshotRegistry(snapshotRegistry).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        snapshotRegistry.idempotentCreateSnapshot(-1);
        assertEquals(new FinalizedControllerFeatures(Map.of("metadata.version", MetadataVersion.MINIMUM_VERSION.featureLevel()), -1),
            manager.finalizedFeatures(-1));
        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 3 for feature " + TestFeatureVersion.FEATURE_NAME + ". Local controller 0 only supports versions 0-2")),
            manager.updateFeatures(updateMap(TestFeatureVersion.FEATURE_NAME, 3),
                Map.of(TestFeatureVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                false, 0));
        ControllerResult<ApiError> result = manager.updateFeatures(
                updateMap(TestFeatureVersion.FEATURE_NAME, 1, "bar", 1), Map.of(),
                false, 0);
        ApiError expectedError = new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 1 for feature bar. Local controller 0 does not support this feature.");
        List<ApiMessageAndVersion> expectedMessages = new ArrayList<>();
        assertEquals(expectedError, result.response());
        assertEquals(expectedMessages, result.records());

        result = manager.updateFeatures(
                updateMap(TestFeatureVersion.FEATURE_NAME, 1), Map.of(),
                false, 0);
        expectedError =  ApiError.NONE;
        assertEquals(expectedError, result.response());
        expectedMessages = new ArrayList<>();
        expectedMessages.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(TestFeatureVersion.FEATURE_NAME).setFeatureLevel((short) 1),
                (short) 0));
        assertEquals(expectedMessages, result.records());
    }

    @Test
    public void testReplay() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureLevelRecord record = new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 2);

        snapshotRegistry.idempotentCreateSnapshot(-1);
        FeatureControlManager manager = new FeatureControlManager.Builder().
                setLogContext(logContext).
                setQuorumFeatures(features("foo", 1, 2)).
                setSnapshotRegistry(snapshotRegistry).
                build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        manager.replay(record);
        snapshotRegistry.idempotentCreateSnapshot(123);
        assertEquals(
            new FinalizedControllerFeatures(versionMap("metadata.version", MetadataVersion.MINIMUM_VERSION.featureLevel(), "foo", 2), 123),
            manager.finalizedFeatures(123));
    }

    @Test
    public void testReplayKraftVersionFeatureLevel() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);

        snapshotRegistry.idempotentCreateSnapshot(-1);
        FeatureControlManager manager = new FeatureControlManager.Builder().
                setLogContext(logContext).
                setQuorumFeatures(features("foo", 1, 2)).
                setSnapshotRegistry(snapshotRegistry).
                build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        // Replay a kraft.version feature level record and shot that the level doesn't get updated
        manager.replay(new FeatureLevelRecord().setName(KRaftVersion.FEATURE_NAME).setFeatureLevel(KRaftVersion.LATEST_PRODUCTION.featureLevel()));
        snapshotRegistry.idempotentCreateSnapshot(123);
        assertEquals(
            new FinalizedControllerFeatures(
                versionMap("metadata.version", MetadataVersion.MINIMUM_VERSION.featureLevel()), 123
            ),
            manager.finalizedFeatures(123)
        );
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
            setQuorumFeatures(features("foo", 1, 5, TransactionVersion.FEATURE_NAME, 0, 3)).
            setSnapshotRegistry(snapshotRegistry).
            setClusterFeatureSupportDescriber(createFakeClusterFeatureSupportDescriber(
                List.of(new SimpleImmutableEntry<>(5, Map.of(TransactionVersion.FEATURE_NAME, VersionRange.of(0, 2)))),
                List.of())).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));

        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 3 for feature foo. Broker 5 does not support this feature.")),
                    manager.updateFeatures(updateMap("foo", 3),
                        Map.of("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                        false, 0));

        ControllerResult<ApiError> result = manager.updateFeatures(
            updateMap(TransactionVersion.FEATURE_NAME, 2), Map.of(), false, 0);
        assertEquals(ApiError.NONE, result.response());
        manager.replay((FeatureLevelRecord) result.records().get(0).message());
        snapshotRegistry.idempotentCreateSnapshot(3);

        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 1 for feature " + TransactionVersion.FEATURE_NAME + "." +
            " Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.")),
            manager.updateFeatures(updateMap(TransactionVersion.FEATURE_NAME, 1), Map.of(), false, 0));

        assertEquals(
            ControllerResult.atomicOf(
                List.of(
                    new ApiMessageAndVersion(
                        new FeatureLevelRecord()
                            .setName(TransactionVersion.FEATURE_NAME)
                            .setFeatureLevel((short) 1),
                        (short) 0
                    )
                ),
                ApiError.NONE
            ),
            manager.updateFeatures(
                updateMap(TransactionVersion.FEATURE_NAME, 1),
                Map.of(TransactionVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                false,
                0)
        );
    }

    @Test
    public void testReplayRecords() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setLogContext(logContext).
            setQuorumFeatures(features(TestFeatureVersion.FEATURE_NAME, 0, 5, TransactionVersion.FEATURE_NAME, 0, 2)).
            setSnapshotRegistry(snapshotRegistry).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        ControllerResult<ApiError> result = manager.
            updateFeatures(updateMap(TestFeatureVersion.FEATURE_NAME, 1, TransactionVersion.FEATURE_NAME, 2), Map.of(), false, 0);
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(MetadataVersion.MINIMUM_VERSION, manager.metadataVersionOrThrow());
        assertEquals(Optional.of((short) 1), manager.finalizedFeatures(Long.MAX_VALUE).get(TestFeatureVersion.FEATURE_NAME));
        assertEquals(Optional.of((short) 2), manager.finalizedFeatures(Long.MAX_VALUE).get(TransactionVersion.FEATURE_NAME));
        assertEquals(Set.of(MetadataVersion.FEATURE_NAME, TestFeatureVersion.FEATURE_NAME, TransactionVersion.FEATURE_NAME),
            manager.finalizedFeatures(Long.MAX_VALUE).featureNames());
    }

    private FeatureControlManager createTestManager() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.IBP_3_6_IV0.featureLevel())).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.IBP_3_4_IV0.featureLevel()));
        return manager;
    }

    @Test
    public void testApplyMetadataVersionChangeRecord() {
        FeatureControlManager manager = createTestManager();
        MetadataVersion initialMetadataVersion = manager.metadataVersionOrThrow();
        manager.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel((short) (initialMetadataVersion.featureLevel() + 1)));
        assertEquals(MetadataVersion.IBP_3_5_IV0, manager.metadataVersionOrThrow());
    }

    @Test
    public void testCannotDowngradeToHigherVersion() {
        FeatureControlManager manager = createTestManager();
        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 9 for feature metadata.version. Can't downgrade to a " +
            "newer version.")),
            manager.updateFeatures(
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_5_IV0.featureLevel()),
                Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                true,
                0));
    }

    @Test
    public void testCannotUnsafeDowngradeToHigherVersion() {
        FeatureControlManager manager = createTestManager();
        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 9 for feature metadata.version. Can't downgrade to a " +
            "newer version.")),
            manager.updateFeatures(
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_5_IV0.featureLevel()),
                Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                true,
                0));
    }

    @Test
    public void testCannotUpgradeToLowerVersion() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.IBP_3_6_IV0.featureLevel())).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.IBP_3_5_IV1.featureLevel()));
        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Invalid update version 9 for feature metadata.version. Can't downgrade the " +
            "version of this feature without setting the upgrade type to either safe or " +
            "unsafe downgrade.")),
            manager.updateFeatures(
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_5_IV0.featureLevel()),
                Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
                true,
                0));
    }

    @Test
    public void testCanUpgradeToHigherVersion() {
        FeatureControlManager manager = createTestManager();
        assertEquals(ControllerResult.of(List.of(), ApiError.NONE),
            manager.updateFeatures(
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_5_IV0.featureLevel()),
                Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
                true,
                0));
    }

    @Test
    public void testCannotUseSafeDowngradeIfMetadataChanged() {
        FeatureControlManager manager = createTestManager();
        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Unsupported metadata.version downgrade from 8 to 7. Refusing to perform the requested downgrade because " +
            "it might delete metadata information.")),
            manager.updateFeatures(
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel()),
                Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                true,
                0));
    }

    @Test
    public void testUnsafeDowngradeIsTemporarilyDisabled() {
        FeatureControlManager manager = createTestManager();
        assertEquals(ControllerResult.of(List.of(), new ApiError(Errors.INVALID_UPDATE_VERSION,
            "Unsupported metadata.version downgrade from 8 to 7. Unsafe metadata downgrade is not supported in this version.")),
                manager.updateFeatures(
                    Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel()),
                    Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                    true,
                    0));
    }

    @Disabled
    @Test
    public void testCanUseUnsafeDowngradeIfMetadataChanged() {
        FeatureControlManager manager = createTestManager();
        assertEquals(ControllerResult.of(List.of(), ApiError.NONE),
                manager.updateFeatures(
                        Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel()),
                        Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                        true,
                        0));
    }

    @Test
    public void testCanUseSafeDowngradeIfMetadataDidNotChange() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
                setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                        MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.IBP_3_6_IV0.featureLevel())).
                build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.IBP_3_5_IV0.featureLevel()));
        assertEquals(ControllerResult.of(List.of(), ApiError.NONE),
                manager.updateFeatures(
                        Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_4_IV0.featureLevel()),
                        Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                        true,
                        0));
    }

    @Test
    public void testCannotDowngradeBeforeMinimumKraftVersion() {
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.latestTesting().featureLevel())).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        assertEquals(
            ControllerResult.of(
                List.of(),
                new ApiError(
                    Errors.INVALID_UPDATE_VERSION,
                    String.format(
                        "Invalid update version 6 for feature metadata.version. Local controller 0 only supports versions %s-%s",
                        MetadataVersion.MINIMUM_VERSION.featureLevel(),
                        MetadataVersion.latestTesting().featureLevel()
                    )
                )
            ),
            manager.updateFeatures(
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersionTestUtils.IBP_3_3_IV2_FEATURE_LEVEL),
                Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                true,
                0
            )
        );
    }

    @Test
    public void testCreateFeatureLevelRecords() {
        Map<String, VersionRange> localSupportedFeatures = new HashMap<>();
        localSupportedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
            MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.latestTesting().featureLevel()));
        localSupportedFeatures.put(Feature.TEST_VERSION.featureName(), VersionRange.of(0, 2));
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0, localSupportedFeatures, List.of())).
            setClusterFeatureSupportDescriber(createFakeClusterFeatureSupportDescriber(
                List.of(new SimpleImmutableEntry<>(1, Map.of(Feature.TEST_VERSION.featureName(), VersionRange.of(0, 3)))),
                List.of())).
                build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        ControllerResult<ApiError> result  = manager.updateFeatures(
                Map.of(Feature.TEST_VERSION.featureName(), (short) 1),
                Map.of(Feature.TEST_VERSION.featureName(), FeatureUpdate.UpgradeType.UPGRADE),
                false,
                0);
        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new FeatureLevelRecord().setName(Feature.TEST_VERSION.featureName()).setFeatureLevel((short) 1), (short) 0)),
                ApiError.NONE), result);
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(Optional.of((short) 1), manager.finalizedFeatures(Long.MAX_VALUE).get(Feature.TEST_VERSION.featureName()));

        ControllerResult<ApiError> result2  = manager.updateFeatures(
                Map.of(Feature.TEST_VERSION.featureName(), (short) 0),
                Map.of(Feature.TEST_VERSION.featureName(), FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                false,
                0);
        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
            new FeatureLevelRecord().setName(Feature.TEST_VERSION.featureName()).setFeatureLevel((short) 0), (short) 0)),
            ApiError.NONE), result2);
        RecordTestUtils.replayAll(manager, result2.records());
        assertEquals(Optional.empty(), manager.finalizedFeatures(Long.MAX_VALUE).get(Feature.TEST_VERSION.featureName()));
    }

    @Test
    public void testUpgradeElrFeatureLevel() {
        Map<String, VersionRange> localSupportedFeatures = new HashMap<>();
        localSupportedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
            MetadataVersion.IBP_4_0_IV1.featureLevel(), MetadataVersion.latestTesting().featureLevel()));
        localSupportedFeatures.put(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.featureName(), VersionRange.of(0, 1));
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0, localSupportedFeatures, List.of())).
            setClusterFeatureSupportDescriber(createFakeClusterFeatureSupportDescriber(
                List.of(new SimpleImmutableEntry<>(1, Map.of(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.featureName(), VersionRange.of(0, 1)))),
                List.of())).
            build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.IBP_4_0_IV1.featureLevel()));
        ControllerResult<ApiError> result = manager.updateFeatures(
            Map.of(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.featureName(), (short) 1),
            Map.of(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.featureName(), FeatureUpdate.UpgradeType.UPGRADE),
            false,
            0);
        assertTrue(result.response().isSuccess());
        assertEquals(List.of(new ApiMessageAndVersion(
            new FeatureLevelRecord().
                setName(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.featureName()).
                setFeatureLevel((short) 1), (short) 0)),
            result.records());
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(Optional.of((short) 1), manager.finalizedFeatures(Long.MAX_VALUE).
            get(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.featureName()));
    }


    @Test
    public void testMetadataVersion() {
        FeatureControlManager manager = new FeatureControlManager.Builder().build();
        assertTrue(manager.metadataVersion().isEmpty());
        assertThrows(IllegalStateException.class, manager::metadataVersionOrThrow);
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        assertTrue(manager.metadataVersion().isPresent());
        assertEquals(MetadataVersion.MINIMUM_VERSION, manager.metadataVersion().get());
        assertEquals(MetadataVersion.MINIMUM_VERSION, manager.metadataVersionOrThrow());
    }
}
