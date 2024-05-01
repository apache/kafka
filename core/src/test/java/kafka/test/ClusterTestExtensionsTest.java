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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@ClusterTestDefaults(clusterType = Type.ZK, serverProperties = {
    @ClusterConfigProperty(key = "default.key", value = "default.value"),
    @ClusterConfigProperty(id = 0, key = "queued.max.requests", value = "100"),
})   // Set defaults for a few params in @ClusterTest(s)
@ExtendWith(ClusterTestExtensions.class)
public class ClusterTestExtensionsTest {

    private final ClusterInstance clusterInstance;

    ClusterTestExtensionsTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    // Static methods can generate cluster configurations
    static void generate1(ClusterGenerator clusterGenerator) {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put("foo", "bar");
        clusterGenerator.accept(ClusterConfig.defaultBuilder()
                .setName("Generated Test")
                .setServerProperties(serverProperties)
                .build());
    }

    // With no params, configuration comes from the annotation defaults as well as @ClusterTestDefaults (if present)
    @ClusterTest
    public void testClusterTest(ClusterInstance clusterInstance) {
        Assertions.assertSame(this.clusterInstance, clusterInstance, "Injected objects should be the same");
        Assertions.assertEquals(ClusterInstance.ClusterType.ZK, clusterInstance.clusterType()); // From the class level default
        Assertions.assertEquals("default.value", clusterInstance.config().serverProperties().get("default.key"));
    }

    // generate1 is a template method which generates any number of cluster configs
    @ClusterTemplate("generate1")
    public void testClusterTemplate() {
        Assertions.assertEquals(ClusterInstance.ClusterType.ZK, clusterInstance.clusterType(),
            "generate1 provided a Zk cluster, so we should see that here");
        Assertions.assertEquals("Generated Test", clusterInstance.config().name().orElse(""),
            "generate1 named this cluster config, so we should see that here");
        Assertions.assertEquals("bar", clusterInstance.config().serverProperties().get("foo"));
    }

    // Multiple @ClusterTest can be used with @ClusterTests
    @ClusterTests({
        @ClusterTest(name = "cluster-tests-1", clusterType = Type.ZK, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "bar"),
            @ClusterConfigProperty(key = "spam", value = "eggs"),
            @ClusterConfigProperty(id = 86400, key = "baz", value = "qux"), // this one will be ignored as there is no broker id is 86400
        }),
        @ClusterTest(name = "cluster-tests-2", clusterType = Type.KRAFT, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "baz"),
            @ClusterConfigProperty(key = "spam", value = "eggz"),
            @ClusterConfigProperty(key = "default.key", value = "overwrite.value"),
            @ClusterConfigProperty(id = 0, key = "queued.max.requests", value = "200"),
            @ClusterConfigProperty(id = 3000, key = "queued.max.requests", value = "300")
        }),
        @ClusterTest(name = "cluster-tests-3", clusterType = Type.CO_KRAFT, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "baz"),
            @ClusterConfigProperty(key = "spam", value = "eggz"),
            @ClusterConfigProperty(key = "default.key", value = "overwrite.value"),
            @ClusterConfigProperty(id = 0, key = "queued.max.requests", value = "200")
        })
    })
    public void testClusterTests() throws ExecutionException, InterruptedException {
        if (clusterInstance.clusterType().equals(ClusterInstance.ClusterType.ZK)) {
            Assertions.assertEquals("bar", clusterInstance.config().serverProperties().get("foo"));
            Assertions.assertEquals("eggs", clusterInstance.config().serverProperties().get("spam"));
            Assertions.assertEquals("default.value", clusterInstance.config().serverProperties().get("default.key"));

            try (Admin admin = clusterInstance.createAdminClient()) {
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
                Map<ConfigResource, Config> configs = admin.describeConfigs(Collections.singletonList(configResource)).all().get();
                Assertions.assertEquals(1, configs.size());
                Assertions.assertEquals("100", configs.get(configResource).get("queued.max.requests").value());
            }
        } else if (clusterInstance.clusterType().equals(ClusterInstance.ClusterType.RAFT)) {
            Assertions.assertEquals("baz", clusterInstance.config().serverProperties().get("foo"));
            Assertions.assertEquals("eggz", clusterInstance.config().serverProperties().get("spam"));
            Assertions.assertEquals("overwrite.value", clusterInstance.config().serverProperties().get("default.key"));

            try (Admin admin = clusterInstance.createAdminClient()) {
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
                Map<ConfigResource, Config> configs = admin.describeConfigs(Collections.singletonList(configResource)).all().get();
                Assertions.assertEquals(1, configs.size());
                Assertions.assertEquals("200", configs.get(configResource).get("queued.max.requests").value());
            }
            if (config.clusterType().equals(Type.KRAFT)) {
                try (Admin admin = Admin.create(Collections.singletonMap(
                        AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, clusterInstance.bootstrapControllers()))) {
                    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "3000");
                    Map<ConfigResource, Config> configs = admin.describeConfigs(Collections.singletonList(configResource)).all().get();
                    Assertions.assertEquals(1, configs.size());
                    Assertions.assertEquals("300", configs.get(configResource).get("queued.max.requests").value());
                }
            }
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
    public void testDefaults(ClusterInstance clusterInstance) {
        Assertions.assertEquals(MetadataVersion.IBP_3_8_IV0, clusterInstance.config().metadataVersion());
    }
}
