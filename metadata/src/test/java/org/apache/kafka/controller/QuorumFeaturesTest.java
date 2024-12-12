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

import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumFeaturesTest {
    private static final Map<String, VersionRange> LOCAL;

    private static final QuorumFeatures QUORUM_FEATURES;

    static {
        Map<String, VersionRange> local = new HashMap<>();
        local.put("foo", VersionRange.of(0, 3));
        local.put("bar", VersionRange.of(0, 4));
        local.put("baz", VersionRange.of(2, 2));
        LOCAL = Collections.unmodifiableMap(local);
        QUORUM_FEATURES = new QuorumFeatures(0, LOCAL, Arrays.asList(0, 1, 2));
    }

    @Test
    public void testDefaultFeatureMap() {
        Map<String, VersionRange> expectedFeatures = new HashMap<>(1);
        expectedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
            MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
            MetadataVersion.LATEST_PRODUCTION.featureLevel()));
        for (Feature feature : Feature.PRODUCTION_FEATURES) {
            short maxVersion = feature.defaultLevel(MetadataVersion.LATEST_PRODUCTION);
            if (maxVersion > 0) {
                expectedFeatures.put(feature.featureName(), VersionRange.of(
                    feature.minimumProduction(),
                    maxVersion
                ));
            }
        }
        assertEquals(expectedFeatures, QuorumFeatures.defaultFeatureMap(false));
    }

    @Test
    public void testDefaultFeatureMapWithUnstable() {
        Map<String, VersionRange> expectedFeatures = new HashMap<>(1);
        expectedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
            MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
            MetadataVersion.latestTesting().featureLevel()));
        for (Feature feature : Feature.PRODUCTION_FEATURES) {
            short maxVersion = feature.defaultLevel(MetadataVersion.latestTesting());
            if (maxVersion > 0) {
                expectedFeatures.put(feature.featureName(), VersionRange.of(
                    feature.minimumProduction(),
                    maxVersion
                ));
            }
        }
        assertEquals(expectedFeatures, QuorumFeatures.defaultFeatureMap(true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void ensureDefaultSupportedFeaturesRangeMaxNotZero(boolean unstableVersionsEnabled) {
        Map<String, VersionRange> quorumFeatures = QuorumFeatures.defaultFeatureMap(unstableVersionsEnabled);
        for (VersionRange range : quorumFeatures.values()) {
            assertNotEquals(0, range.max());
        }
    }

    @Test
    public void testLocalSupportedFeature() {
        assertEquals(VersionRange.of(0, 3), QUORUM_FEATURES.localSupportedFeature("foo"));
        assertEquals(VersionRange.of(0, 4), QUORUM_FEATURES.localSupportedFeature("bar"));
        assertEquals(VersionRange.of(2, 2), QUORUM_FEATURES.localSupportedFeature("baz"));
        assertEquals(VersionRange.of(0, 0), QUORUM_FEATURES.localSupportedFeature("quux"));
    }

    @Test
    public void testReasonNotSupported() {
        assertEquals(Optional.of("Local controller 0 only supports versions 0-3"),
            QuorumFeatures.reasonNotSupported((short) 10,
                "Local controller 0", VersionRange.of(0, 3)));
        assertEquals(Optional.empty(),
            QuorumFeatures.reasonNotSupported((short) 3,
                "Local controller 0", VersionRange.of(0, 3)));
    }

    @Test
    public void testIsControllerId() {
        assertTrue(QUORUM_FEATURES.isControllerId(0));
        assertTrue(QUORUM_FEATURES.isControllerId(1));
        assertTrue(QUORUM_FEATURES.isControllerId(2));
        assertFalse(QUORUM_FEATURES.isControllerId(3));
    }
}
