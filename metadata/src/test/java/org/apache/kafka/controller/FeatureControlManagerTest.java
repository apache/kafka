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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
        Map<String, VersionRange> features = QuorumFeatures.defaultFeatureMap();
        features.putAll(rangeMap(args));
        return new QuorumFeatures(0, new ApiVersions(), features, Collections.emptyList());
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
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        snapshotRegistry.getOrCreateSnapshot(-1);
        FeatureControlManager manager = new FeatureControlManager(logContext,
            features("foo", 1, 2), snapshotRegistry);
        assertEquals(new FinalizedControllerFeatures(Collections.emptyMap(), -1),
            manager.finalizedFeatures(-1));
        assertEquals(ControllerResult.atomicOf(Collections.emptyList(), Collections.
                singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Invalid update version 3 for feature foo. The quorum does not support the given feature version."))),
            manager.updateFeatures(updateMap("foo", 3),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                Collections.emptyMap(), false));
        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
                updateMap("foo", 2, "bar", 1), Collections.emptyMap(),
                Collections.emptyMap(), false);
        Map<String, ApiError> expectedMap = new HashMap<>();
        expectedMap.put("foo", ApiError.NONE);
        expectedMap.put("bar", new ApiError(Errors.INVALID_UPDATE_VERSION,
                "Invalid update version 1 for feature bar. The controller does not support the given feature."));
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
        FeatureControlManager manager = new FeatureControlManager(logContext,
            features("foo", 1, 2), snapshotRegistry);
        manager.replay(record);
        snapshotRegistry.getOrCreateSnapshot(123);
        assertEquals(new FinalizedControllerFeatures(versionMap("foo", 2), 123),
            manager.finalizedFeatures(123));
    }

    @Test
    public void testUpdateFeaturesErrorCases() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager(logContext,
            features("foo", 1, 5, "bar", 1, 2), snapshotRegistry);

        assertEquals(
            ControllerResult.atomicOf(
                Collections.emptyList(),
                Collections.singletonMap(
                    "foo",
                    new ApiError(
                        Errors.INVALID_UPDATE_VERSION,
                        "Invalid update version 3 for feature foo. Broker 5 does not support this feature."
                    )
                )
            ),
            manager.updateFeatures(
                updateMap("foo", 3),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                Collections.singletonMap(5, rangeMap()),
                false)
        );

        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
            updateMap("foo", 3), Collections.emptyMap(), Collections.emptyMap(), false);
        assertEquals(Collections.singletonMap("foo", ApiError.NONE), result.response());
        manager.replay((FeatureLevelRecord) result.records().get(0).message());
        snapshotRegistry.getOrCreateSnapshot(3);

        assertEquals(ControllerResult.atomicOf(Collections.emptyList(), Collections.
                singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Invalid update version 2 for feature foo. Can't downgrade the version of this feature " +
                    "without setting the upgrade type to either safe or unsafe downgrade."))),
            manager.updateFeatures(updateMap("foo", 2),
                Collections.emptyMap(), Collections.emptyMap(), false));

        assertEquals(
            ControllerResult.atomicOf(
                Collections.singletonList(
                    new ApiMessageAndVersion(
                        new FeatureLevelRecord()
                            .setName("foo")
                            .setFeatureLevel((short) 2),
                        (short) 0
                    )
                ),
                Collections.singletonMap("foo", ApiError.NONE)
            ),
            manager.updateFeatures(
                updateMap("foo", 2),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                Collections.emptyMap(),
                false)
        );
    }

    @Test
    public void testFeatureControlIterator() throws Exception {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager(logContext,
            features("foo", 1, 5, "bar", 1, 2), snapshotRegistry);
        ControllerResult<Map<String, ApiError>> result = manager.
            updateFeatures(updateMap("foo", 5, "bar", 1),
                Collections.emptyMap(), Collections.emptyMap(), false);
        RecordTestUtils.replayAll(manager, result.records());
        RecordTestUtils.assertBatchIteratorContains(Arrays.asList(
            Arrays.asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName("foo").
                setFeatureLevel((short) 5), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName("bar").
                setFeatureLevel((short) 1), (short) 0))),
            manager.iterator(Long.MAX_VALUE));
    }

    @Test
    public void testInitializeMetadataVersion() {
        // Default QuorumFeatures
        checkMetadataVersion(features(), MetadataVersion.IBP_3_0_IV0, Errors.NONE);
        checkMetadataVersion(features(), MetadataVersion.latest(), Errors.NONE);
        checkMetadataVersion(features(), MetadataVersion.UNINITIALIZED, Errors.INVALID_UPDATE_VERSION);
        checkMetadataVersion(features(), MetadataVersion.IBP_2_7_IV1, Errors.INVALID_UPDATE_VERSION);

        // Increased QuorumFeatures
        QuorumFeatures features = features(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV0.featureLevel());
        checkMetadataVersion(features, MetadataVersion.IBP_3_0_IV0, Errors.INVALID_UPDATE_VERSION);

        // Empty QuorumFeatures
        features = new QuorumFeatures(0, new ApiVersions(), Collections.emptyMap(), Collections.emptyList());
        checkMetadataVersion(features, MetadataVersion.latest(), Errors.INVALID_UPDATE_VERSION);
        checkMetadataVersion(features, MetadataVersion.IBP_3_0_IV0, Errors.INVALID_UPDATE_VERSION);
    }

    @Test
    public void reInitializeMetadataVersion() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager(logContext, features(), snapshotRegistry);
        ControllerResult<Map<String, ApiError>> result = manager.initializeMetadataVersion(MetadataVersion.IBP_3_0_IV0.featureLevel());
        Errors actual = result.response().get(MetadataVersion.FEATURE_NAME).error();
        assertEquals(Errors.NONE, actual);
        RecordTestUtils.replayAll(manager, result.records());

        result = manager.initializeMetadataVersion(MetadataVersion.latest().featureLevel());
        actual = result.response().get(MetadataVersion.FEATURE_NAME).error();
        assertEquals(Errors.INVALID_UPDATE_VERSION, actual);
    }

    public void checkMetadataVersion(QuorumFeatures features, MetadataVersion version, Errors expected) {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager(logContext, features, snapshotRegistry);
        ControllerResult<Map<String, ApiError>> result = manager.initializeMetadataVersion(version.featureLevel());
        Errors actual = result.response().get(MetadataVersion.FEATURE_NAME).error();
        assertEquals(expected, actual);
    }

    @Test
    public void testDowngradeMetadataVersion() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        QuorumFeatures features = features(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV0.featureLevel());
        FeatureControlManager manager = new FeatureControlManager(logContext, features, snapshotRegistry);
        ControllerResult<Map<String, ApiError>> result = manager.initializeMetadataVersion(MetadataVersion.IBP_3_3_IV0.featureLevel());
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(manager.metadataVersion(), MetadataVersion.IBP_3_3_IV0);

        result = manager.updateFeatures(
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel()),
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
            Collections.emptyMap(),
            true);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().get(MetadataVersion.FEATURE_NAME).error());


        result = manager.updateFeatures(
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel()),
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
            Collections.emptyMap(),
            true);
        assertEquals(Errors.NONE, result.response().get(MetadataVersion.FEATURE_NAME).error());

        result = manager.updateFeatures(
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_0_IV0.featureLevel()),
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                Collections.emptyMap(),
                true);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().get(MetadataVersion.FEATURE_NAME).error());
        assertEquals("Invalid update version 1 for feature metadata.version. The quorum does not support the given feature version.",
            result.response().get(MetadataVersion.FEATURE_NAME).message());
    }
}
