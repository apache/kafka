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

import static org.apache.kafka.server.common.Feature.validateDefaultValueAndLatestProductionValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FeatureTest {
    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testV0SupportedInEarliestMV(Feature feature) {
        assertTrue(feature.featureVersions().length >= 1);
        assertEquals(MetadataVersion.MINIMUM_KRAFT_VERSION,
            feature.featureVersions()[0].bootstrapMetadataVersion());
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testFromFeatureLevelAllFeatures(Feature feature) {
        FeatureVersion[] featureImplementations = feature.featureVersions();
        int numFeatures = featureImplementations.length;
        short latestProductionLevel = feature.latestProduction();

        for (short i = 0; i < numFeatures; i++) {
            short level = i;
            if (latestProductionLevel < i) {
                assertEquals(featureImplementations[i], feature.fromFeatureLevel(level, true));
                assertThrows(IllegalArgumentException.class, () -> feature.fromFeatureLevel(level, false));
            } else {
                assertEquals(featureImplementations[i], feature.fromFeatureLevel(level, false));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testValidateVersionAllFeatures(Feature feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            // Ensure the minimum bootstrap metadata version is included if no metadata version dependency.
            Map<String, Short> deps = new HashMap<>();
            deps.putAll(featureImpl.dependencies());
            if (!deps.containsKey(MetadataVersion.FEATURE_NAME)) {
                deps.put(MetadataVersion.FEATURE_NAME, MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.featureLevel());
            }

            // Ensure that the feature is valid given the typical metadataVersionMapping and the dependencies.
            // Note: Other metadata versions are valid, but this one should always be valid.
            Feature.validateVersion(featureImpl, deps);
        }
    }

    @Test
    public void testInvalidValidateVersion() {
        // No MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Feature.validateVersion(
                TestFeatureVersion.TEST_1,
                Collections.emptyMap()
            )
        );

        // Using too low of a MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Feature.validateVersion(
                TestFeatureVersion.TEST_1,
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_2_8_IV0.featureLevel())
            )
        );

        // Using a version that is lower than the dependency will fail.
        assertThrows(IllegalArgumentException.class,
             () -> Feature.validateVersion(
                 TestFeatureVersion.TEST_2,
                 Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())
             )
        );
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testDefaultLevelAllFeatures(Feature feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            // If features have the same bootstrapMetadataVersion, the highest level feature should be chosen.
            short defaultLevel = feature.defaultLevel(featureImpl.bootstrapMetadataVersion());
            if (defaultLevel != featureImpl.featureLevel()) {
                FeatureVersion otherFeature = feature.fromFeatureLevel(defaultLevel, true);
                assertEquals(featureImpl.bootstrapMetadataVersion(), otherFeature.bootstrapMetadataVersion());
                assertTrue(defaultLevel > featureImpl.featureLevel());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testLatestProductionIsOneOfFeatureValues(Feature feature) {
        assertTrue(feature.hasFeatureVersion(feature.latestProduction));
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testLatestProductionIsNotBehindLatestMetadataVersion(Feature feature) {
        assertTrue(feature.latestProduction() >= feature.defaultLevel(MetadataVersion.latestProduction()));
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testLatestProductionDependencyIsProductionReady(Feature feature) {
        for (Map.Entry<String, Short> dependency: feature.latestProduction.dependencies().entrySet()) {
            String featureName = dependency.getKey();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                Feature dependencyFeature = Feature.featureFromName(featureName);
                assertTrue(dependencyFeature.isProductionReady(dependency.getValue()));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Feature.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testDefaultVersionDependencyIsDefaultReady(Feature feature) {
        for (Map.Entry<String, Short> dependency: feature.defaultVersion(MetadataVersion.LATEST_PRODUCTION).dependencies().entrySet()) {
            String featureName = dependency.getKey();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                Feature dependencyFeature = Feature.featureFromName(featureName);
                assertTrue(dependency.getValue() <= dependencyFeature.defaultLevel(MetadataVersion.LATEST_PRODUCTION));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testDefaultTestVersion(MetadataVersion metadataVersion) {
        short expectedVersion;
        if (!metadataVersion.isLessThan(MetadataVersion.latestTesting())) {
            expectedVersion = 2;
        } else if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_7_IV0)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, Feature.TEST_VERSION.defaultLevel(metadataVersion));
    }

    @Test
    public void testUnstableTestVersion() {
        // If the latest MetadataVersion is stable, we don't throw an error. In that case, we don't worry about unstable feature
        // versions since all feature versions are stable.
        if (MetadataVersion.latestProduction().isLessThan(MetadataVersion.latestTesting())) {
            assertThrows(IllegalArgumentException.class, () ->
                Feature.TEST_VERSION.fromFeatureLevel(Feature.TEST_VERSION.latestTesting(), false));
        }
        Feature.TEST_VERSION.fromFeatureLevel(Feature.TEST_VERSION.latestTesting(), true);
    }

    @Test
    public void testValidateWithNonExistentLatestProduction() {
        assertThrows(IllegalArgumentException.class, () ->
            validateDefaultValueAndLatestProductionValue(Feature.UNIT_TEST_VERSION_0),
            "Feature UNIT_TEST_VERSION_0 has latest production version UT_FV0_1 " +
                "which is not one of its feature versions.");
    }

    @Test
    public void testValidateWithLaggingLatestProduction() {
        assertThrows(IllegalArgumentException.class, () ->
            validateDefaultValueAndLatestProductionValue(Feature.UNIT_TEST_VERSION_1),
            "Feature UNIT_TEST_VERSION_1 has latest production value UT_FV1_0 " +
                "smaller than its default version UT_FV1_1 with latest production MV.");
    }

    @Test
    public void testValidateWithDependencyNotProductionReady() {
        assertThrows(IllegalArgumentException.class, () ->
                validateDefaultValueAndLatestProductionValue(Feature.UNIT_TEST_VERSION_3),
            "Feature UNIT_TEST_VERSION_3 has latest production FeatureVersion UT_FV3_1 with dependency " +
                "UT_FV2_1 that is not production ready. (UNIT_TEST_VERSION_2 latest production: UT_FV2_0)");
    }

    @Test
    public void testValidateWithDefaultValueDependencyAheadOfItsDefaultLevel() {
        if (MetadataVersion.latestProduction().isLessThan(MetadataVersion.latestTesting())) {
            assertThrows(IllegalArgumentException.class, () ->
                    validateDefaultValueAndLatestProductionValue(Feature.UNIT_TEST_VERSION_5),
                "Feature UNIT_TEST_VERSION_5 has default FeatureVersion UT_FV5_1 when MV=3.7-IV0 with " +
                    "dependency UT_FV4_1 that is behind its default version UT_FV4_0.");
        }
    }

    @Test
    public void testValidateWithMVDependencyNotProductionReady() {
        if (MetadataVersion.latestProduction().isLessThan(MetadataVersion.latestTesting())) {
            assertThrows(IllegalArgumentException.class, () ->
                    validateDefaultValueAndLatestProductionValue(Feature.UNIT_TEST_VERSION_6),
                "Feature UNIT_TEST_VERSION_6 has latest production FeatureVersion UT_FV6_1 with " +
                    "MV dependency 4.0-IV3 that is not production ready. (MV latest production: 4.0-IV0)");
        }
    }

    @Test
    public void testValidateWithMVDependencyAheadOfBootstrapMV() {
        assertThrows(IllegalArgumentException.class, () ->
                validateDefaultValueAndLatestProductionValue(Feature.UNIT_TEST_VERSION_7),
            "Feature UNIT_TEST_VERSION_7 has default FeatureVersion UT_FV7_0 when MV=3.0-IV1 with " +
                "MV dependency 3.7-IV0 that is behind its bootstrap MV 3.0-IV1.");
    }
}
