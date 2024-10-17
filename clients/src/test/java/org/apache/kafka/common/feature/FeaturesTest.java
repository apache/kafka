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

package org.apache.kafka.common.feature;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FeaturesTest {

    @Test
    public void testEmptyFeatures() {
        Map<String, Map<String, Short>> emptyMap = new HashMap<>();

        Features<SupportedVersionRange> emptySupportedFeatures = Features.emptySupportedFeatures();
        assertTrue(emptySupportedFeatures.features().isEmpty());
        assertTrue(emptySupportedFeatures.toMap().isEmpty());
        assertEquals(emptySupportedFeatures, Features.fromSupportedFeaturesMap(emptyMap));
    }

    @Test
    public void testNullFeatures() {
        assertThrows(
            NullPointerException.class,
            () -> Features.supportedFeatures(null));
    }

    @Test
    public void testGetAllFeaturesAPI() {
        SupportedVersionRange v1 = new SupportedVersionRange((short) 1, (short) 2);
        SupportedVersionRange v2 = new SupportedVersionRange((short) 3, (short) 4);
        Map<String, SupportedVersionRange> allFeatures =
            mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));
        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);
        assertEquals(allFeatures, features.features());
    }

    @Test
    public void testGetAPI() {
        SupportedVersionRange v1 = new SupportedVersionRange((short) 1, (short) 2);
        SupportedVersionRange v2 = new SupportedVersionRange((short) 3, (short) 4);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));
        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);
        assertEquals(v1, features.get("feature_1"));
        assertEquals(v2, features.get("feature_2"));
        assertNull(features.get("nonexistent_feature"));
    }

    @Test
    public void testFromFeaturesMapToFeaturesMap() {
        SupportedVersionRange v1 = new SupportedVersionRange((short) 1, (short) 2);
        SupportedVersionRange v2 = new SupportedVersionRange((short) 3, (short) 4);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));

        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);

        Map<String, Map<String, Short>> expected = mkMap(
            mkEntry("feature_1", mkMap(mkEntry("min_version", (short) 1), mkEntry("max_version", (short) 2))),
            mkEntry("feature_2", mkMap(mkEntry("min_version", (short) 3), mkEntry("max_version", (short) 4))));
        assertEquals(expected, features.toMap());
        assertEquals(features, Features.fromSupportedFeaturesMap(expected));
    }

    @Test
    public void testToStringSupportedFeatures() {
        SupportedVersionRange v1 = new SupportedVersionRange((short) 1, (short) 2);
        SupportedVersionRange v2 = new SupportedVersionRange((short) 3, (short) 4);
        Map<String, SupportedVersionRange> allFeatures
            = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));

        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);

        assertEquals(
            "Features{(feature_1 -> SupportedVersionRange[min_version:1, max_version:2]), (feature_2 -> SupportedVersionRange[min_version:3, max_version:4])}",
            features.toString());
    }

    @Test
    public void testSupportedFeaturesFromMapFailureWithInvalidMissingMaxVersion() {
        // This is invalid because 'max_version' key is missing.
        Map<String, Map<String, Short>> invalidFeatures = mkMap(
            mkEntry("feature_1", mkMap(mkEntry("min_version", (short) 1))));
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.fromSupportedFeaturesMap(invalidFeatures));
    }

    @Test
    public void testEquals() {
        SupportedVersionRange v1 = new SupportedVersionRange((short) 1, (short) 2);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1));
        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);
        Features<SupportedVersionRange> featuresClone = Features.supportedFeatures(allFeatures);
        assertEquals(features, featuresClone);

        SupportedVersionRange v2 = new SupportedVersionRange((short) 1, (short) 3);
        Map<String, SupportedVersionRange> allFeaturesDifferent = mkMap(mkEntry("feature_1", v2));
        Features<SupportedVersionRange> featuresDifferent = Features.supportedFeatures(allFeaturesDifferent);
        assertNotEquals(features, featuresDifferent);

        assertNotEquals(null, features);
    }
}
