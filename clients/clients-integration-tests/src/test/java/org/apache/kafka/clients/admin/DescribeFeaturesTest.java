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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.GroupVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.ShareVersion;
import org.apache.kafka.server.common.StreamsVersion;
import org.apache.kafka.server.common.TransactionVersion;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DescribeFeaturesTest {

    @ClusterTest(
        types = {Type.KRAFT},
        metadataVersion = MetadataVersion.IBP_3_7_IV0,
        controllers = 2,
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(
                id = 3000,
                key = "unstable.api.versions.enable",
                value = "true"
                ),
            @ClusterConfigProperty(
                id = 3001,
                key = "unstable.api.versions.enable",
                value = "false"
                ),
            @ClusterConfigProperty(
                id = 0,
                key = "unstable.feature.versions.enable",
                value = "true"
                ),
            @ClusterConfigProperty(
                id = 1,
                key = "unstable.feature.versions.enable",
                value = "false"
                )
        }
    )
    public void testUnstableApiVersions(ClusterInstance clusterInstance) {
        // Test unstable.api.versions.enable on controller nodes
        try (Admin admin = clusterInstance.admin(Map.of(), true)) {
            // unstable.api.versions.enable is true on node 3000
            assertFeatures(admin, 3000, true, clusterInstance.config().metadataVersion());

            // unstable.api.versions.enable is false on node 3001
            assertFeatures(admin, 3001, false, clusterInstance.config().metadataVersion());
        }

        // Test unstable.feature.versions.enable on broker nodes
        try (Admin admin = clusterInstance.admin()) {
            // unstable.feature.versions.enable is true on node 0
            assertFeatures(admin, 0, true, clusterInstance.config().metadataVersion());

            // unstable.feature.versions.enable is false on node 1
            assertFeatures(admin, 1, false, clusterInstance.config().metadataVersion());
        }
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testSendRequestToWrongNodeType(ClusterInstance clusterInstance) {
        try (Admin admin = clusterInstance.admin()) {
            // use bootstrap-servers to send request to controller
            assertThrows(
                ExecutionException.class,
                () -> admin.describeFeatures(new DescribeFeaturesOptions().nodeId(3000).timeoutMs(1000)).featureMetadata().get());
        }

        try (Admin admin = clusterInstance.admin(Map.of(), true)) {
            // use bootstrap-controllers to send request to broker
            assertThrows(
                ExecutionException.class,
                () -> admin.describeFeatures(new DescribeFeaturesOptions().nodeId(0).timeoutMs(1000)).featureMetadata().get());
        }
    }

    private void assertFeatures(Admin admin, int nodeId, boolean unstable, MetadataVersion metadataVersion) {
        FeatureMetadata featureMetadata = assertDoesNotThrow(
            () -> admin.describeFeatures(new DescribeFeaturesOptions().nodeId(nodeId)).featureMetadata().get());

        assertEquals(Map.of(
            MetadataVersion.FEATURE_NAME, new FinalizedVersionRange(metadataVersion.featureLevel(), metadataVersion.featureLevel())
        ), featureMetadata.finalizedFeatures());

        if (unstable) {
            assertEquals(Map.of(
                GroupVersion.FEATURE_NAME, new SupportedVersionRange(Feature.GROUP_VERSION.minimumProduction(), Feature.GROUP_VERSION.latestTesting()),
                KRaftVersion.FEATURE_NAME, new SupportedVersionRange(Feature.KRAFT_VERSION.minimumProduction(), Feature.KRAFT_VERSION.latestTesting()),
                MetadataVersion.FEATURE_NAME, new SupportedVersionRange(MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.latestTesting().featureLevel()),
                ShareVersion.FEATURE_NAME, new SupportedVersionRange(Feature.SHARE_VERSION.minimumProduction(), Feature.SHARE_VERSION.latestTesting()),
                StreamsVersion.FEATURE_NAME, new SupportedVersionRange(Feature.STREAMS_VERSION.minimumProduction(), Feature.STREAMS_VERSION.latestTesting()),
                TransactionVersion.FEATURE_NAME, new SupportedVersionRange(Feature.TRANSACTION_VERSION.minimumProduction(), Feature.TRANSACTION_VERSION.latestTesting()),
                EligibleLeaderReplicasVersion.FEATURE_NAME, new SupportedVersionRange(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.minimumProduction(), Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.latestTesting())
            ), featureMetadata.supportedFeatures());
        } else {
            assertEquals(Map.of(
                GroupVersion.FEATURE_NAME, new SupportedVersionRange(Feature.GROUP_VERSION.minimumProduction(), Feature.GROUP_VERSION.latestProduction()),
                KRaftVersion.FEATURE_NAME, new SupportedVersionRange(Feature.KRAFT_VERSION.minimumProduction(), Feature.KRAFT_VERSION.latestProduction()),
                MetadataVersion.FEATURE_NAME, new SupportedVersionRange(MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.latestProduction().featureLevel()),
                ShareVersion.FEATURE_NAME, new SupportedVersionRange(Feature.SHARE_VERSION.minimumProduction(), Feature.SHARE_VERSION.latestProduction()),
                StreamsVersion.FEATURE_NAME, new SupportedVersionRange(Feature.STREAMS_VERSION.minimumProduction(), Feature.STREAMS_VERSION.latestProduction()),
                TransactionVersion.FEATURE_NAME, new SupportedVersionRange(Feature.TRANSACTION_VERSION.minimumProduction(), Feature.TRANSACTION_VERSION.latestProduction()),
                EligibleLeaderReplicasVersion.FEATURE_NAME, new SupportedVersionRange(Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.minimumProduction(), Feature.ELIGIBLE_LEADER_REPLICAS_VERSION.latestProduction())
            ), featureMetadata.supportedFeatures());
        }
    }
}
