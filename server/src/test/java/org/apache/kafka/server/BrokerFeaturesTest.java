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

import static org.apache.kafka.server.common.Features.ELIGIBLE_LEADER_REPLICAS_VERSION;
import static org.apache.kafka.server.common.Features.GROUP_VERSION;
import static org.apache.kafka.server.common.Features.TRANSACTION_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerFeaturesTest {

    @Test
    public void testEmpty() {
        assertTrue(BrokerFeatures.createEmpty().supportedFeatures().empty());
    }

    @Test
    public void testIncompatibilitiesDueToAbsentFeature() {
        Map<String, SupportedVersionRange> newFeatures = new HashMap<>();
        newFeatures.put("test_feature_1", new SupportedVersionRange((short) 1, (short) 4));
        newFeatures.put("test_feature_2", new SupportedVersionRange((short) 1, (short) 3));
        Features<SupportedVersionRange> supportedFeatures = Features.supportedFeatures(newFeatures);
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(true, supportedFeatures);

        Map<String, Short> compatibleFeatures = new HashMap<>();
        compatibleFeatures.put("test_feature_1", (short) 4);
        Map<String, Short> inCompatibleFeatures = new HashMap<>();
        inCompatibleFeatures.put("test_feature_2", (short) 4);

        Map<String, Short> finalizedFeatures = new HashMap<>(compatibleFeatures);
        finalizedFeatures.putAll(inCompatibleFeatures);

        assertEquals(inCompatibleFeatures,
                brokerFeatures.incompatibleFeatures(finalizedFeatures));
        assertTrue(BrokerFeatures.hasIncompatibleFeatures(supportedFeatures, finalizedFeatures));
    }

    @Test
    public void testIncompatibilitiesDueToIncompatibleFeature() {
        Map<String, SupportedVersionRange> newFeatures = new HashMap<>();
        newFeatures.put("test_feature_1", new SupportedVersionRange((short) 1, (short) 4));
        newFeatures.put("test_feature_2", new SupportedVersionRange((short) 1, (short) 3));
        Features<SupportedVersionRange> supportedFeatures = Features.supportedFeatures(newFeatures);
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(true, supportedFeatures);

        Map<String, Short> compatibleFeatures = new HashMap<>();
        compatibleFeatures.put("test_feature_1", (short) 3);
        Map<String, Short> inCompatibleFeatures = new HashMap<>();
        inCompatibleFeatures.put("test_feature_2", (short) 4);
        Map<String, Short> finalizedFeatures = new HashMap<>(compatibleFeatures);
        finalizedFeatures.putAll(inCompatibleFeatures);

        assertEquals(inCompatibleFeatures, brokerFeatures.incompatibleFeatures(finalizedFeatures));
        assertTrue(BrokerFeatures.hasIncompatibleFeatures(supportedFeatures, finalizedFeatures));
    }

    @Test
    public void testCompatibleFeatures() {
        Map<String, SupportedVersionRange> newFeatures = new HashMap<>();
        newFeatures.put("test_feature_1", new SupportedVersionRange((short) 1, (short) 4));
        newFeatures.put("test_feature_2", new SupportedVersionRange((short) 1, (short) 3));
        Features<SupportedVersionRange> supportedFeatures = Features.supportedFeatures(newFeatures);
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(true, supportedFeatures);

        Map<String, Short> compatibleFeatures = new HashMap<>();
        compatibleFeatures.put("test_feature_1", (short) 3);
        compatibleFeatures.put("test_feature_2", (short) 3);
        Map<String, Short> finalizedFeatures = new HashMap<>(compatibleFeatures);

        assertTrue(brokerFeatures.incompatibleFeatures(finalizedFeatures).isEmpty());
        assertFalse(BrokerFeatures.hasIncompatibleFeatures(supportedFeatures, finalizedFeatures));
    }

    @Test
    public void testDefaultFinalizedFeatures() {
        Map<String, SupportedVersionRange> newFeatures = new HashMap<>();
        newFeatures.put("test_feature_1", new SupportedVersionRange((short) 1, (short) 4));
        newFeatures.put("test_feature_2", new SupportedVersionRange((short) 1, (short) 3));
        newFeatures.put("test_feature_3", new SupportedVersionRange((short) 3, (short) 7));
        Features<SupportedVersionRange> supportedFeatures = Features.supportedFeatures(newFeatures);
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(true, supportedFeatures);

        Map<String, Short> expectedFeatures = new HashMap<>();
        expectedFeatures.put(MetadataVersion.FEATURE_NAME, MetadataVersion.latestTesting().featureLevel());
        expectedFeatures.put(TRANSACTION_VERSION.featureName(), TRANSACTION_VERSION.latestTesting());
        expectedFeatures.put(GROUP_VERSION.featureName(), GROUP_VERSION.latestTesting());
        expectedFeatures.put(ELIGIBLE_LEADER_REPLICAS_VERSION.featureName(), ELIGIBLE_LEADER_REPLICAS_VERSION.latestTesting());
        expectedFeatures.put("kraft.version", (short) 0);
        expectedFeatures.put("test_feature_1", (short) 4);
        expectedFeatures.put("test_feature_2", (short) 3);
        expectedFeatures.put("test_feature_3", (short) 7);

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
