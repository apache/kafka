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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.metadata.VersionRange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumFeaturesTest {
    @Test
    public void testQuorumFeatures() {
        ApiVersions apiVersions = new ApiVersions();
        Map<String, VersionRange> featureMap = new HashMap<>(2);
        featureMap.put("foo", VersionRange.of(1, 2));
        featureMap.put("bar", VersionRange.of(3, 5));

        List<Integer> nodeIds = new ArrayList<>();
        nodeIds.add(0);

        QuorumFeatures quorumFeatures = new QuorumFeatures(0, apiVersions, featureMap, nodeIds);
        assertLocalFeature(quorumFeatures, "foo", 1, 2);
        assertLocalFeature(quorumFeatures, "bar", 3, 5);
        assertQuorumFeature(quorumFeatures, "foo", 1, 2);
        assertQuorumFeature(quorumFeatures, "bar", 3, 5);

        // Add a second node with identical features
        nodeIds.add(1);
        apiVersions.update("1", nodeApiVersions(featureMap));
        assertLocalFeature(quorumFeatures, "foo", 1, 2);
        assertLocalFeature(quorumFeatures, "bar", 3, 5);
        assertQuorumFeature(quorumFeatures, "foo", 1, 2);
        assertQuorumFeature(quorumFeatures, "bar", 3, 5);

        // Change the supported features of one node
        Map<String, VersionRange> node1Features = new HashMap<>(featureMap);
        node1Features.put("bar", VersionRange.of(3, 4));
        apiVersions.update("1", nodeApiVersions(node1Features));
        assertLocalFeature(quorumFeatures, "foo", 1, 2);
        assertLocalFeature(quorumFeatures, "bar", 3, 5);
        assertQuorumFeature(quorumFeatures, "foo", 1, 2);
        assertQuorumFeature(quorumFeatures, "bar", 3, 4);

        // Add a third node with no features
        nodeIds.add(2);
        apiVersions.update("1", NodeApiVersions.create());
        assertFalse(quorumFeatures.quorumSupportedFeature("foo").isPresent());
        assertFalse(quorumFeatures.quorumSupportedFeature("bar").isPresent());
    }


    public static NodeApiVersions nodeApiVersions(Map<String, VersionRange> featureMap) {
        List<ApiVersionsResponseData.SupportedFeatureKey> supportedFeatures = new ArrayList<>(featureMap.size());
        featureMap.forEach((featureName, versionRange) -> {
            supportedFeatures.add(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName(featureName)
                .setMinVersion(versionRange.min())
                .setMaxVersion(versionRange.max()));
        });
        return new NodeApiVersions(Collections.emptyList(), supportedFeatures);
    }

    private void assertLocalFeature(QuorumFeatures features, String name, int expectedMin, int expectedMax) {
        Optional<VersionRange> featureRange = features.localSupportedFeature(name);
        assertTrue(featureRange.isPresent());
        assertEquals(expectedMin, featureRange.get().min());
        assertEquals(expectedMax, featureRange.get().max());
    }

    private void assertQuorumFeature(QuorumFeatures features, String name, int expectedMin, int expectedMax) {
        Optional<VersionRange> featureRange = features.quorumSupportedFeature(name);
        assertTrue(featureRange.isPresent());
        assertEquals(expectedMin, featureRange.get().min());
        assertEquals(expectedMax, featureRange.get().max());
    }

}
