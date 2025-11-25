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
                )
        }
    )
    public void testUnstableApiVersions(ClusterInstance clusterInstance) {
        try (Admin admin = clusterInstance.admin(Map.of(), true)) {
            // unstable.api.versions.enable is true on node 3000
            FeatureMetadata featureMetadata = assertDoesNotThrow(
                () -> admin.describeFeatures(new DescribeFeaturesOptions().nodeId(3000)).featureMetadata().get());
            assertEquals(Map.of(
                GroupVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                KRaftVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                MetadataVersion.FEATURE_NAME, new SupportedVersionRange(MetadataVersion.IBP_3_3_IV3.featureLevel(), MetadataVersion.IBP_4_2_IV1.featureLevel()),
                ShareVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                StreamsVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                TransactionVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 2),
                EligibleLeaderReplicasVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1)
            ), featureMetadata.supportedFeatures());

            // unstable.api.versions.enable is false on node 3001
            featureMetadata = assertDoesNotThrow(
                () -> admin.describeFeatures(new DescribeFeaturesOptions().nodeId(3001)).featureMetadata().get());
            assertEquals(Map.of(
                GroupVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                KRaftVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                MetadataVersion.FEATURE_NAME, new SupportedVersionRange(MetadataVersion.IBP_3_3_IV3.featureLevel(), MetadataVersion.IBP_4_1_IV1.featureLevel()),
                ShareVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                StreamsVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1),
                TransactionVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 2),
                EligibleLeaderReplicasVersion.FEATURE_NAME, new SupportedVersionRange((short) 0, (short) 1)
            ), featureMetadata.supportedFeatures());
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
}
