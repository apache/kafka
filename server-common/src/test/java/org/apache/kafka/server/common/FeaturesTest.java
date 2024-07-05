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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FeaturesTest {

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testFromFeatureLevelAllFeatures(Features feature) {
        FeatureVersion[] featureImplementations = feature.featureVersions();
        int numFeatures = featureImplementations.length;
        short latestProductionLevel = feature.latestProduction();

        for (short i = 1; i < numFeatures; i++) {
            short level = i;
            if (latestProductionLevel < i) {
                assertEquals(featureImplementations[i - 1], feature.fromFeatureLevel(level, true));
                assertThrows(IllegalArgumentException.class, () -> feature.fromFeatureLevel(level, false));
            } else {
                assertEquals(featureImplementations[i - 1], feature.fromFeatureLevel(level, false));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testValidateVersionAllFeatures(Features feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            // Ensure the minimum bootstrap metadata version is included if no metadata version dependency.
            Map<String, Short> deps = new HashMap<>();
            deps.putAll(featureImpl.dependencies());
            if (!deps.containsKey(MetadataVersion.FEATURE_NAME)) {
                deps.put(MetadataVersion.FEATURE_NAME, MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.featureLevel());
            }

            // Ensure that the feature is valid given the typical metadataVersionMapping and the dependencies.
            // Note: Other metadata versions are valid, but this one should always be valid.
            Features.validateVersion(featureImpl, deps);
        }
    }

    @Test
    public void testInvalidValidateVersion() {
        // No MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Features.validateVersion(
                TestFeatureVersion.TEST_1,
                Collections.emptyMap()
            )
        );

        // Using too low of a MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Features.validateVersion(
                TestFeatureVersion.TEST_1,
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_2_8_IV0.featureLevel())
            )
        );

        // Using a version that is lower than the dependency will fail.
        assertThrows(IllegalArgumentException.class,
             () -> Features.validateVersion(
                 TestFeatureVersion.TEST_2,
                 Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())
             )
        );
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testDefaultValueAllFeatures(Features feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            assertEquals(feature.defaultValue(featureImpl.bootstrapMetadataVersion()), featureImpl.featureLevel(),
                    "Failed to get the correct default for " + featureImpl);
        }
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testLatestProductionMapsToLatestMetadataVersion(Features features) {
        assertEquals(features.latestProduction(), features.defaultValue(MetadataVersion.LATEST_PRODUCTION));
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testDefaultTestVersion(MetadataVersion metadataVersion) {
        short expectedVersion;
        if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_9_IV0)) {
            expectedVersion = 2;
        } else if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_7_IV0)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, Features.TEST_VERSION.defaultValue(metadataVersion));
    }

    @Test
    public void testUnstableTestVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            Features.TEST_VERSION.fromFeatureLevel(Features.TEST_VERSION.latestTesting(), false));
        Features.TEST_VERSION.fromFeatureLevel(Features.TEST_VERSION.latestTesting(), true);
    }
}
