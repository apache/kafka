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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class FeaturesTest {

    @Test
    public void testEmptyFeatures() {
        Map<String, Map<String, Long>> emptyMap = new HashMap<>();

        Features<FinalizedVersionRange> emptyFinalizedFeatures = Features.emptyFinalizedFeatures();
        assertTrue(emptyFinalizedFeatures.features().isEmpty());
        assertTrue(emptyFinalizedFeatures.serialize().isEmpty());
        assertEquals(emptyFinalizedFeatures, Features.deserializeFinalizedFeatures(emptyMap));

        Features<SupportedVersionRange> emptySupportedFeatures = Features.emptySupportedFeatures();
        assertTrue(emptySupportedFeatures.features().isEmpty());
        assertTrue(emptySupportedFeatures.serialize().isEmpty());
        assertEquals(emptySupportedFeatures, Features.deserializeSupportedFeatures(emptyMap));
    }

    @Test
    public void testNullFeatures() {
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.finalizedFeatures(null));
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.supportedFeatures(null));
    }

    @Test
    public void testGetAllFeaturesAPI() {
        SupportedVersionRange v1 = new SupportedVersionRange(1, 2);
        SupportedVersionRange v2 = new SupportedVersionRange(3, 4);
        Map<String, SupportedVersionRange> allFeatures =
            mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));
        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);
        assertEquals(allFeatures, features.features());
    }

    @Test
    public void testGetAPI() {
        SupportedVersionRange v1 = new SupportedVersionRange(1, 2);
        SupportedVersionRange v2 = new SupportedVersionRange(3, 4);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));
        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);
        assertEquals(v1, features.get("feature_1"));
        assertEquals(v2, features.get("feature_2"));
        assertNull(features.get("nonexistent_feature"));
    }

    @Test
    public void testSerializeDeserializeSupportedFeatures() {
        SupportedVersionRange v1 = new SupportedVersionRange(1, 2);
        SupportedVersionRange v2 = new SupportedVersionRange(3, 4);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));

        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);

        Map<String, Map<String, Long>> expected = mkMap(
            mkEntry("feature_1", mkMap(mkEntry("min_version", 1L), mkEntry("max_version", 2L))),
            mkEntry("feature_2", mkMap(mkEntry("min_version", 3L), mkEntry("max_version", 4L))));
        assertEquals(expected, features.serialize());
        assertEquals(features, Features.deserializeSupportedFeatures(expected));
    }

    @Test
    public void testSerializeDeserializeFinalizedFeatures() {
        FinalizedVersionRange v1 = new FinalizedVersionRange(1, 2);
        FinalizedVersionRange v2 = new FinalizedVersionRange(3, 4);
        Map<String, FinalizedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));

        Features<FinalizedVersionRange> features = Features.finalizedFeatures(allFeatures);

        Map<String, Map<String, Long>> expected = mkMap(
            mkEntry("feature_1", mkMap(mkEntry("min_version_level", 1L), mkEntry("max_version_level", 2L))),
            mkEntry("feature_2", mkMap(mkEntry("min_version_level", 3L), mkEntry("max_version_level", 4L))));
        assertEquals(expected, features.serialize());
        assertEquals(features, Features.deserializeFinalizedFeatures(expected));
    }

    @Test
    public void testToStringFinalizedFeatures() {
        FinalizedVersionRange v1 = new FinalizedVersionRange(1, 2);
        FinalizedVersionRange v2 = new FinalizedVersionRange(3, 4);
        Map<String, FinalizedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));

        Features<FinalizedVersionRange> features = Features.finalizedFeatures(allFeatures);

        assertEquals(
            "Features{(feature_1 -> FinalizedVersionRange[1, 2]), (feature_2 -> FinalizedVersionRange[3, 4])}",
            features.toString());
    }

    @Test
    public void testToStringSupportedFeatures() {
        SupportedVersionRange v1 = new SupportedVersionRange(1, 2);
        SupportedVersionRange v2 = new SupportedVersionRange(3, 4);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1), mkEntry("feature_2", v2));

        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);

        assertEquals(
            "Features{(feature_1 -> SupportedVersionRange[1, 2]), (feature_2 -> SupportedVersionRange[3, 4])}",
            features.toString());
    }

    @Test
    public void testDeserializationFailureSupportedFeatures() {
        Map<String, Map<String, Long>> invalidFeatures = mkMap(
            mkEntry("feature_1", mkMap(mkEntry("min_version", 1L))));
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.deserializeSupportedFeatures(invalidFeatures));
    }

    @Test
    public void testDeserializationFailureFinalizedFeatures() {
        Map<String, Map<String, Long>> invalidFeatures = mkMap(
            mkEntry("feature_1", mkMap(mkEntry("min_version", 1L))));
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.deserializeFinalizedFeatures(invalidFeatures));
    }

    @Test
    public void testEquals() {
        SupportedVersionRange v1 = new SupportedVersionRange(1, 2);
        Map<String, SupportedVersionRange> allFeatures = mkMap(mkEntry("feature_1", v1));
        Features<SupportedVersionRange> features = Features.supportedFeatures(allFeatures);
        Features<SupportedVersionRange> featuresClone = Features.supportedFeatures(allFeatures);
        assertTrue(features.equals(featuresClone));

        SupportedVersionRange v2 = new SupportedVersionRange(1, 3);
        Map<String, SupportedVersionRange> allFeaturesDifferent = mkMap(mkEntry("feature_1", v2));
        Features<SupportedVersionRange> featuresDifferent = Features.supportedFeatures(allFeaturesDifferent);
        assertFalse(features.equals(featuresDifferent));

        assertFalse(features.equals(null));
    }
}
