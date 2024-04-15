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

package kafka.test;

import kafka.test.annotation.AutoStart;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTemplate;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.ClusterTests;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;


@ClusterTestDefaults(clusterType = Type.ZK)   // Set defaults for a few params in @ClusterTest(s)
@ExtendWith(ClusterTestExtensions.class)
public class ClusterTestExtensionsTest {

    private final ClusterInstance clusterInstance;
    private final ClusterConfig config;

    ClusterTestExtensionsTest(ClusterInstance clusterInstance, ClusterConfig config) {     // Constructor injections
        this.clusterInstance = clusterInstance;
        this.config = config;
    }

    // Static methods can generate cluster configurations
    static void generate1(ClusterGenerator clusterGenerator) {
        clusterGenerator.accept(ClusterConfig.defaultClusterBuilder().name("Generated Test").build());
    }

    // BeforeEach run after class construction, but before cluster initialization and test invocation
    @BeforeEach
    public void beforeEach(ClusterConfig config) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
        config.serverProperties().put("before", "each");
    }

    // AfterEach runs after test invocation and cluster teardown
    @AfterEach
    public void afterEach(ClusterConfig config) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
    }

    // With no params, configuration comes from the annotation defaults as well as @ClusterTestDefaults (if present)
    @ClusterTest
    public void testClusterTest(ClusterConfig config, ClusterInstance clusterInstance) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
        Assertions.assertSame(this.clusterInstance, clusterInstance, "Injected objects should be the same");
        Assertions.assertEquals(clusterInstance.clusterType(), ClusterInstance.ClusterType.ZK); // From the class level default
        Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("before"), "each");
    }

    // generate1 is a template method which generates any number of cluster configs
    @ClusterTemplate("generate1")
    public void testClusterTemplate() {
        Assertions.assertEquals(clusterInstance.clusterType(), ClusterInstance.ClusterType.ZK,
            "generate1 provided a Zk cluster, so we should see that here");
        Assertions.assertEquals(clusterInstance.config().name().orElse(""), "Generated Test",
            "generate1 named this cluster config, so we should see that here");
        Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("before"), "each");
    }

    // Multiple @ClusterTest can be used with @ClusterTests
    @ClusterTests({
        @ClusterTest(name = "cluster-tests-1", clusterType = Type.ZK, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "bar"),
            @ClusterConfigProperty(key = "spam", value = "eggs")
        }),
        @ClusterTest(name = "cluster-tests-2", clusterType = Type.KRAFT, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "baz"),
            @ClusterConfigProperty(key = "spam", value = "eggz")
        }),
        @ClusterTest(name = "cluster-tests-3", clusterType = Type.CO_KRAFT, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "baz"),
            @ClusterConfigProperty(key = "spam", value = "eggz")
        })
    })
    public void testClusterTests() {
        if (clusterInstance.clusterType().equals(ClusterInstance.ClusterType.ZK)) {
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("foo"), "bar");
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("spam"), "eggs");
        } else if (clusterInstance.clusterType().equals(ClusterInstance.ClusterType.RAFT)) {
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("foo"), "baz");
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("spam"), "eggz");
        } else {
            Assertions.fail("Unknown cluster type " + clusterInstance.clusterType());
        }
    }

    @ClusterTest(autoStart = AutoStart.NO)
    public void testNoAutoStart() {
        Assertions.assertThrows(RuntimeException.class, clusterInstance::anyBrokerSocketServer);
        clusterInstance.start();
        Assertions.assertNotNull(clusterInstance.anyBrokerSocketServer());
    }

    @ClusterTest
    public void testDefaults(ClusterConfig config) {
        Assertions.assertEquals(MetadataVersion.IBP_3_8_IV0, config.metadataVersion());
    }
}
