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
package kafka.server;

import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.StopMirrorTopicsRequest;
import org.apache.kafka.common.requests.StopMirrorTopicsResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StopMirrorTopicsRequestIntegrationTest {
    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_4_5_IV1,
        features = {@ClusterFeature(feature = Feature.MIRROR_VERSION, version = 1)})
    void testBrokerForwardingAndControllerStub(ClusterInstance cluster) throws Exception {
        cluster.waitForReadyBrokers();
        for (int port : List.of(cluster.brokerBoundPorts().get(0), cluster.controllerBoundPorts().get(0))) {
            StopMirrorTopicsResponse response = IntegrationTestUtils.connectAndReceive(request(), port);
            assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data().errorCode());
            assertEquals("StopMirrorTopics is not implemented yet.", response.data().errorMessage());
        }
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_4_5_IV1,
        features = {@ClusterFeature(feature = Feature.MIRROR_VERSION, version = 0)})
    void testFeatureDisabledOnBrokerAndController(ClusterInstance cluster) throws Exception {
        cluster.waitForReadyBrokers();
        for (int port : List.of(cluster.brokerBoundPorts().get(0), cluster.controllerBoundPorts().get(0))) {
            StopMirrorTopicsResponse response = IntegrationTestUtils.connectAndReceive(request(), port);
            assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data().errorCode());
            assertEquals("Cluster mirroring is disabled (mirror.version=0).", response.data().errorMessage());
        }
    }

    @ClusterTest(types = {Type.KRAFT},
        serverProperties = {@ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false")})
    void testUnstableApiIsNotAdvertised(ClusterInstance cluster) throws Exception {
        ApiVersionsResponse response = IntegrationTestUtils.connectAndReceive(
            new ApiVersionsRequest.Builder().build(), cluster.brokerBoundPorts().get(0));
        assertNull(response.data().apiKeys().find(ApiKeys.STOP_MIRROR_TOPICS.id));
    }

    private static StopMirrorTopicsRequest request() {
        return new StopMirrorTopicsRequest.Builder(new StopMirrorTopicsRequestData()
            .setMirrorName("mirror-a")
            .setTopicPatterns(List.of("topic-.*"))
            .setTopics(null)
            .setTimeoutMs(30_000), true).build();
    }
}
