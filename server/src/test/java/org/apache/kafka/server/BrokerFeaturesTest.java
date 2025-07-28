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
package org.apache.kafka.server;

import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.common.Feature.ELIGIBLE_LEADER_REPLICAS_VERSION;
import static org.apache.kafka.server.common.Feature.GROUP_VERSION;
import static org.apache.kafka.server.common.Feature.SHARE_VERSION;
import static org.apache.kafka.server.common.Feature.STREAMS_VERSION;
import static org.apache.kafka.server.common.Feature.TRANSACTION_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerFeaturesTest {
    private static final Features<SupportedVersionRange> SUPPORTED_FEATURES = Features.supportedFeatures(Map.of(
            "test_feature_1", new SupportedVersionRange((short) 1, (short) 4),
            "test_feature_2", new SupportedVersionRange((short) 1, (short) 3)
    ));

    private static final BrokerFeatures BROKER_FEATURES = BrokerFeatures.createDefault(true, SUPPORTED_FEATURES);

    @Test
    public void testEmpty() {
        assertTrue(BrokerFeatures.createEmpty().supportedFeatures().empty());
    }

    @Test
    public void testIncompatibilitiesDueToAbsentFeature() {
        Map<String, Short> compatibleFeatures = Map.of("test_feature_1", (short) 4);
        Map<String, Short> inCompatibleFeatures = Map.of("test_feature_2", (short) 4);

        Map<String, Short> finalizedFeatures = new HashMap<>(compatibleFeatures);
        finalizedFeatures.putAll(inCompatibleFeatures);

        assertEquals(inCompatibleFeatures, BROKER_FEATURES.incompatibleFeatures(finalizedFeatures));
        assertTrue(BrokerFeatures.hasIncompatibleFeatures(SUPPORTED_FEATURES, finalizedFeatures));
    }

    @Test
    public void testIncompatibilitiesDueToIncompatibleFeature() {
        Map<String, Short> compatibleFeatures = Map.of("test_feature_1", (short) 3);
        Map<String, Short> inCompatibleFeatures = Map.of("test_feature_2", (short) 4);
        Map<String, Short> finalizedFeatures = new HashMap<>(compatibleFeatures);
        finalizedFeatures.putAll(inCompatibleFeatures);

        assertEquals(inCompatibleFeatures, BROKER_FEATURES.incompatibleFeatures(finalizedFeatures));
        assertTrue(BrokerFeatures.hasIncompatibleFeatures(SUPPORTED_FEATURES, finalizedFeatures));
    }

    @Test
    public void testCompatibleFeatures() {
        Map<String, Short> compatibleFeatures = Map.of(
                "test_feature_1", (short) 3,
                "test_feature_2", (short) 3
        );

        assertTrue(BROKER_FEATURES.incompatibleFeatures(compatibleFeatures).isEmpty());
        assertFalse(BrokerFeatures.hasIncompatibleFeatures(SUPPORTED_FEATURES, compatibleFeatures));
    }

    @Test
    public void testDefaultFinalizedFeatures() {
        Map<String, SupportedVersionRange> newFeatures = Map.of(
                "test_feature_1", new SupportedVersionRange((short) 1, (short) 4),
                "test_feature_2", new SupportedVersionRange((short) 1, (short) 3),
                "test_feature_3", new SupportedVersionRange((short) 3, (short) 7)
        );
        Features<SupportedVersionRange> supportedFeatures = Features.supportedFeatures(newFeatures);
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(true, supportedFeatures);

        Map<String, Short> expectedFeatures = Map.of(
                MetadataVersion.FEATURE_NAME, MetadataVersion.latestTesting().featureLevel(),
                TRANSACTION_VERSION.featureName(), TRANSACTION_VERSION.latestTesting(),
                GROUP_VERSION.featureName(), GROUP_VERSION.latestTesting(),
                ELIGIBLE_LEADER_REPLICAS_VERSION.featureName(), ELIGIBLE_LEADER_REPLICAS_VERSION.latestTesting(),
                SHARE_VERSION.featureName(), SHARE_VERSION.latestTesting(),
                STREAMS_VERSION.featureName(), STREAMS_VERSION.latestTesting(),
                "kraft.version", (short) 0,
                "test_feature_1", (short) 4,
                "test_feature_2", (short) 3,
                "test_feature_3", (short) 7
        );

        assertEquals(expectedFeatures, brokerFeatures.defaultFinalizedFeatures());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void ensureDefaultSupportedFeaturesRangeMaxNotZero(boolean unstableVersionsEnabled) {
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(unstableVersionsEnabled);
        brokerFeatures.supportedFeatures().features()
                .values()
                .forEach(supportedVersionRange -> assertNotEquals(0, supportedVersionRange.max()));
    }
}
