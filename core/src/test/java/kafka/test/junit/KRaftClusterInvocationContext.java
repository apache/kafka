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

package kafka.test.junit;

import kafka.test.ClusterConfig;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Wraps a {@link KafkaClusterTestKit} inside lifecycle methods for a test invocation. Each instance of this
 * class is provided with a configuration for the cluster.
 *
 * This context also provides parameter resolvers for:
 *
 * <ul>
 *     <li>ClusterConfig (the same instance passed to the constructor)</li>
 *     <li>ClusterInstance (includes methods to expose underlying SocketServer-s)</li>
 *     <li>IntegrationTestHelper (helper methods)</li>
 * </ul>
 */
public class KRaftClusterInvocationContext implements TestTemplateInvocationContext {

    private final ClusterConfig clusterConfig;
    private final AtomicReference<KafkaClusterTestKit> clusterReference;

    public KRaftClusterInvocationContext(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        return String.format("[%d] Type=Raft, %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        KRaftClusterInstance clusterInstance = new KRaftClusterInstance(clusterReference, clusterConfig);
        return Arrays.asList(
            (BeforeTestExecutionCallback) context -> {
                TestKitNodes nodes = new TestKitNodes.Builder().
                        setNumBrokerNodes(clusterConfig.numBrokers()).
                        setNumControllerNodes(clusterConfig.numControllers()).build();
                nodes.brokerNodes().forEach((brokerId, brokerNode) -> {
                    clusterConfig.brokerServerProperties(brokerId).forEach(
                            (key, value) -> brokerNode.propertyOverrides().put(key.toString(), value.toString()));
                });
                KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);

                // Copy properties into the TestKit builder
                clusterConfig.serverProperties().forEach((key, value) -> builder.setConfigProp(key.toString(), value.toString()));
                // KAFKA-12512 need to pass security protocol and listener name here
                KafkaClusterTestKit cluster = builder.build();
                clusterReference.set(cluster);
                cluster.format();
                cluster.startup();
                kafka.utils.TestUtils.waitUntilTrue(
                    () -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                    () -> "Broker never made it to RUNNING state.",
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                    100L);
            },
            (AfterTestExecutionCallback) context -> clusterInstance.stop(),
            new ClusterInstanceParameterResolver(clusterInstance),
            new GenericParameterResolver<>(clusterConfig, ClusterConfig.class)
        );
    }

}
