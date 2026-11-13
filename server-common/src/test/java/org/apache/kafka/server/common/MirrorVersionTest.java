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

import org.apache.kafka.common.feature.SupportedVersionRange;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MirrorVersionTest {
    @ParameterizedTest
    @EnumSource(MirrorVersion.class)
    void testFeatureNameAndLevel(MirrorVersion version) {
        assertEquals("mirror.version", version.featureName());
        assertEquals(version, MirrorVersion.fromFeatureLevel(version.featureLevel()));
        assertEquals(version, Feature.MIRROR_VERSION.fromFeatureLevel(version.featureLevel(), true));
    }

    @Test
    void testFeatureLevels() {
        assertEquals(0, MirrorVersion.MV_0.featureLevel());
        assertEquals(1, MirrorVersion.MV_1.featureLevel());
    }

    @ParameterizedTest
    @ValueSource(shorts = {-1, 2, Short.MAX_VALUE})
    void testUnknownFeatureLevel(short level) {
        assertThrows(RuntimeException.class, () -> MirrorVersion.fromFeatureLevel(level));
        assertThrows(IllegalArgumentException.class, () -> Feature.MIRROR_VERSION.fromFeatureLevel(level, true));
    }

    @Test
    void testDisabledByDefaultInProduction() {
        assertEquals(MirrorVersion.MV_0, MirrorVersion.LATEST_PRODUCTION);
        assertEquals(0, Feature.MIRROR_VERSION.defaultLevel(MetadataVersion.latestProduction()));
        assertEquals(0, Feature.MIRROR_VERSION.latestProduction());
        assertEquals(1, Feature.MIRROR_VERSION.latestTesting());
        assertEquals(Feature.MIRROR_VERSION, Feature.featureFromName("mirror.version"));
    }

    @Test
    void testUnstableVersionRequiresOptIn() {
        assertEquals(new SupportedVersionRange((short) 0, (short) 0), Feature.MIRROR_VERSION.supportedVersionRange(false));
        assertEquals(new SupportedVersionRange((short) 0, (short) 1), Feature.MIRROR_VERSION.supportedVersionRange(true));
        assertThrows(IllegalArgumentException.class, () -> Feature.MIRROR_VERSION.fromFeatureLevel((short) 1, false));
        assertEquals(MirrorVersion.MV_1, Feature.MIRROR_VERSION.fromFeatureLevel((short) 1, true));
    }

    @Test
    void testMetadataVersionMapping() {
        assertEquals(MetadataVersion.MINIMUM_VERSION, MirrorVersion.MV_0.bootstrapMetadataVersion());
        assertEquals(MetadataVersion.IBP_4_5_IV1, MirrorVersion.MV_1.bootstrapMetadataVersion());
        assertEquals(Map.of(), MirrorVersion.MV_0.dependencies());
        assertEquals(Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_5_IV1.featureLevel()),
            MirrorVersion.MV_1.dependencies());
    }

    @Test
    void testUnstableMetadataVersionMapping() {
        assertEquals(0, Feature.MIRROR_VERSION.defaultLevel(MetadataVersion.IBP_4_5_IV0));
        assertEquals(1, Feature.MIRROR_VERSION.defaultLevel(MetadataVersion.IBP_4_5_IV1));
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    void testMetadataVersionDependency(MetadataVersion metadataVersion) {
        Map<String, Short> features = Map.of(MetadataVersion.FEATURE_NAME, metadataVersion.featureLevel());
        assertDoesNotThrow(() -> Feature.validateVersion(MirrorVersion.MV_0, features));
        if (metadataVersion.isAtLeast(MetadataVersion.IBP_4_5_IV1)) {
            assertDoesNotThrow(() -> Feature.validateVersion(MirrorVersion.MV_1, features));
        } else {
            assertThrows(IllegalArgumentException.class, () -> Feature.validateVersion(MirrorVersion.MV_1, features));
        }
    }

    @Test
    void testMissingMetadataVersionDependency() {
        assertThrows(IllegalArgumentException.class, () -> Feature.validateVersion(MirrorVersion.MV_1, Map.of()));
    }

    @Test
    void testIsEnabled() {
        assertFalse(MirrorVersion.isEnabled(Map.of()));
        assertFalse(MirrorVersion.isEnabled(Map.of(MirrorVersion.FEATURE_NAME, MirrorVersion.MV_0.featureLevel())));
        assertTrue(MirrorVersion.isEnabled(Map.of(MirrorVersion.FEATURE_NAME, MirrorVersion.MV_1.featureLevel())));
    }
}
