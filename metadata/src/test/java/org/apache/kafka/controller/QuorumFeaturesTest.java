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
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.metadata.VersionRange;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumFeaturesTest {
    private final static Map<String, VersionRange> LOCAL;

    static {
        Map<String, VersionRange> local = new HashMap<>();
        local.put("foo", VersionRange.of(0, 3));
        local.put("bar", VersionRange.of(0, 4));
        local.put("baz", VersionRange.of(2, 2));
        LOCAL = Collections.unmodifiableMap(local);
    }

    @Test
    public void testDefaultSupportedLevels() {
        QuorumFeatures quorumFeatures = new QuorumFeatures(0, new ApiVersions(), emptyMap(), Arrays.asList(0, 1, 2));
        assertEquals(Optional.empty(), quorumFeatures.reasonNotSupported("foo", (short) 0));
        assertEquals(Optional.of("Local controller 0 does not support this feature."),
            quorumFeatures.reasonNotSupported("foo", (short) 1));
    }

    @Test
    public void testLocalSupportedFeature() {
        QuorumFeatures quorumFeatures = new QuorumFeatures(0, new ApiVersions(), LOCAL, Arrays.asList(0, 1, 2));
        assertEquals(VersionRange.of(0, 3), quorumFeatures.localSupportedFeature("foo"));
        assertEquals(VersionRange.of(0, 4), quorumFeatures.localSupportedFeature("bar"));
        assertEquals(VersionRange.of(2, 2), quorumFeatures.localSupportedFeature("baz"));
        assertEquals(VersionRange.of(0, 0), quorumFeatures.localSupportedFeature("quux"));
    }

    @Test
    public void testReasonNotSupported() {
        ApiVersions apiVersions = new ApiVersions();
        QuorumFeatures quorumFeatures = new QuorumFeatures(0, apiVersions, LOCAL, Arrays.asList(0, 1, 2));
        assertEquals(Optional.of("Local controller 0 only supports versions 0-3"),
                quorumFeatures.reasonNotSupported("foo", (short) 10));
        apiVersions.update("1", nodeApiVersions(Arrays.asList(
                new SimpleImmutableEntry<>("foo", VersionRange.of(1, 3)),
                new SimpleImmutableEntry<>("bar", VersionRange.of(1, 3)),
                new SimpleImmutableEntry<>("baz", VersionRange.of(1, 2)))));
        assertEquals(Optional.empty(), quorumFeatures.reasonNotSupported("bar", (short) 3));
        assertEquals(Optional.of("Controller 1 only supports versions 1-3"),
                quorumFeatures.reasonNotSupported("bar", (short) 4));
    }

    private static NodeApiVersions nodeApiVersions(List<Entry<String, VersionRange>> entries) {
        List<SupportedFeatureKey> features = new ArrayList<>();
        entries.forEach(entry -> {
            features.add(new SupportedFeatureKey().
                    setName(entry.getKey()).
                    setMinVersion(entry.getValue().min()).
                    setMaxVersion(entry.getValue().max()));
        });
        return new NodeApiVersions(Collections.emptyList(), features, false);
    }

    @Test
    public void testIsControllerId() {
        QuorumFeatures quorumFeatures = new QuorumFeatures(0, new ApiVersions(), LOCAL, Arrays.asList(0, 1, 2));
        assertTrue(quorumFeatures.isControllerId(0));
        assertTrue(quorumFeatures.isControllerId(1));
        assertTrue(quorumFeatures.isControllerId(2));
        assertFalse(quorumFeatures.isControllerId(3));
    }

    @Test
    public void testZkMigrationReady() {
        ApiVersions apiVersions = new ApiVersions();
        QuorumFeatures quorumFeatures = new QuorumFeatures(0, apiVersions, LOCAL, Arrays.asList(0, 1, 2));

        // create apiVersion with zkMigrationEnabled flag set for node 0, the other 2 nodes have no apiVersions info
        apiVersions.update("0", new NodeApiVersions(Collections.emptyList(), Collections.emptyList(), true));
        assertTrue(quorumFeatures.reasonAllControllersZkMigrationNotReady().isPresent());
        assertTrue(quorumFeatures.reasonAllControllersZkMigrationNotReady().get().contains("Missing apiVersion from nodes: [1, 2]"));

        // create apiVersion with zkMigrationEnabled flag set for node 1, the other 1 node have no apiVersions info
        apiVersions.update("1", new NodeApiVersions(Collections.emptyList(), Collections.emptyList(), true));
        assertTrue(quorumFeatures.reasonAllControllersZkMigrationNotReady().isPresent());
        assertTrue(quorumFeatures.reasonAllControllersZkMigrationNotReady().get().contains("Missing apiVersion from nodes: [2]"));

        // create apiVersion with zkMigrationEnabled flag disabled for node 2, should still be not ready
        apiVersions.update("2", NodeApiVersions.create());
        assertTrue(quorumFeatures.reasonAllControllersZkMigrationNotReady().isPresent());
        assertTrue(quorumFeatures.reasonAllControllersZkMigrationNotReady().get().contains("Nodes don't enable `zookeeper.metadata.migration.enable`: [2]"));

        // update zkMigrationEnabled flag to enabled for node 2, should be ready now
        apiVersions.update("2", new NodeApiVersions(Collections.emptyList(), Collections.emptyList(), true));
        assertFalse(quorumFeatures.reasonAllControllersZkMigrationNotReady().isPresent());

        // create apiVersion with zkMigrationEnabled flag disabled for a non-controller, and expect we fill filter it out
        apiVersions.update("3", NodeApiVersions.create());
        assertFalse(quorumFeatures.reasonAllControllersZkMigrationNotReady().isPresent());
    }
}
