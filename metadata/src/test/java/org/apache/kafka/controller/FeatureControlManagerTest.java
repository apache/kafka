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
import java.util.Optional;

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

import static java.util.Collections.emptyList;
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
        return new QuorumFeatures(0, new ApiVersions(), features, emptyList());
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
            build();
        snapshotRegistry.getOrCreateSnapshot(-1);
        assertEquals(new FinalizedControllerFeatures(Collections.emptyMap(), -1),
            manager.finalizedFeatures(-1));
        assertEquals(ControllerResult.atomicOf(emptyList(), Collections.
                singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Invalid update version 3 for feature foo. Local controller 0 only supports versions 1-2"))),
            manager.updateFeatures(updateMap("foo", 3),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                Collections.emptyMap(), false));
        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
                updateMap("foo", 2, "bar", 1), Collections.emptyMap(),
                Collections.emptyMap(), false);
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
                build();
        manager.replay(record);
        snapshotRegistry.getOrCreateSnapshot(123);
        assertEquals(new FinalizedControllerFeatures(versionMap("foo", 2), 123),
            manager.finalizedFeatures(123));
    }

    @Test
    public void testUpdateFeaturesErrorCases() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setLogContext(logContext).
            setQuorumFeatures(features("foo", 1, 5, "bar", 1, 2)).
            setSnapshotRegistry(snapshotRegistry).
            build();

        assertEquals(
            ControllerResult.atomicOf(
                emptyList(),
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

        assertEquals(ControllerResult.atomicOf(emptyList(), Collections.
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
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setLogContext(logContext).
            setQuorumFeatures(features("foo", 1, 5, "bar", 1, 2)).
            setSnapshotRegistry(snapshotRegistry).
            build();
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
    public void testApplyMetadataVersionChangeRecord() {
        QuorumFeatures features = features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.IBP_3_0_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV0.featureLevel());
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features).build();
        manager.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_0_IV0.featureLevel()));
        assertEquals(MetadataVersion.IBP_3_0_IV0, manager.metadataVersion());
    }

    @Test
    public void testDowngradeMetadataVersion() {
        QuorumFeatures features = features(MetadataVersion.FEATURE_NAME,
                MetadataVersion.IBP_3_2_IV0.featureLevel(), MetadataVersion.IBP_3_3_IV0.featureLevel());
        FeatureControlManager manager = new FeatureControlManager.Builder().
            setQuorumFeatures(features).
            setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
            build();
        assertEquals(manager.metadataVersion(), MetadataVersion.IBP_3_3_IV0);

        ControllerResult<Map<String, ApiError>> result;
        result = manager.updateFeatures(
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_2_IV0.featureLevel()),
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.UPGRADE),
            Collections.emptyMap(),
            true);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().get(MetadataVersion.FEATURE_NAME).error());


        result = manager.updateFeatures(
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_1_IV0.featureLevel()),
            Collections.singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
            Collections.emptyMap(),
            true);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().get(MetadataVersion.FEATURE_NAME).error());

        result = manager.updateFeatures(
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_0_IV0.featureLevel()),
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                Collections.emptyMap(),
                true);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().get(MetadataVersion.FEATURE_NAME).error());
        assertEquals("Invalid update version 1 for feature metadata.version. Local controller 0 only supports versions 4-5",
            result.response().get(MetadataVersion.FEATURE_NAME).message());
    }

    @Test
    public void testCreateFeatureLevelRecords() {
        Map<String, VersionRange> localSupportedFeatures = new HashMap<>();
        localSupportedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
                MetadataVersion.IBP_3_0_IV0.featureLevel(), MetadataVersion.latest().featureLevel()));
        localSupportedFeatures.put("foo", VersionRange.of(0, 2));
        FeatureControlManager manager = new FeatureControlManager.Builder().
                setQuorumFeatures(new QuorumFeatures(0, new ApiVersions(), localSupportedFeatures, emptyList())).
                build();
        ControllerResult<Map<String, ApiError>> result  = manager.updateFeatures(
                Collections.singletonMap("foo", (short) 1),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.UPGRADE),
                Collections.singletonMap(1, Collections.singletonMap("foo", VersionRange.of(0, 3))),
                false);
        assertEquals(ControllerResult.atomicOf(Arrays.asList(new ApiMessageAndVersion(
                new FeatureLevelRecord().setName("foo").setFeatureLevel((short) 1), (short) 0)),
                        Collections.singletonMap("foo", ApiError.NONE)), result);
        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(Optional.of((short) 1), manager.finalizedFeatures(Long.MAX_VALUE).get("foo"));

        ControllerResult<Map<String, ApiError>> result2  = manager.updateFeatures(
                Collections.singletonMap("foo", (short) 0),
                Collections.singletonMap("foo", FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE),
                Collections.singletonMap(1, Collections.singletonMap("foo", VersionRange.of(0, 3))),
                false);
        assertEquals(ControllerResult.atomicOf(Arrays.asList(new ApiMessageAndVersion(
                        new FeatureLevelRecord().setName("foo").setFeatureLevel((short) 0), (short) 0)),
                Collections.singletonMap("foo", ApiError.NONE)), result2);
        RecordTestUtils.replayAll(manager, result2.records());
        assertEquals(Optional.empty(), manager.finalizedFeatures(Long.MAX_VALUE).get("foo"));
    }
}
