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

package kafka.test.server;

import kafka.server.ControllerServer;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BootstrapControllersIntegrationTest {
    private KafkaClusterTestKit cluster;

    private String bootstrapControllerString;

    @BeforeAll
    public void createCluster() throws Exception {
        this.cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(3).
                setNumControllerNodes(3).build()).build();
        this.cluster.format();
        this.cluster.startup();
        this.cluster.waitForActiveController();
        this.cluster.waitForReadyBrokers();
        StringBuilder bootstrapControllerStringBuilder = new StringBuilder();
        String prefix = "";
        for (ControllerServer controller : cluster.controllers().values()) {
            bootstrapControllerStringBuilder.append(prefix);
            prefix = ",";
            int port = controller.socketServerFirstBoundPortFuture().get(1, TimeUnit.MINUTES);
            bootstrapControllerStringBuilder.append("localhost:").append(port);
        }
        bootstrapControllerString = bootstrapControllerStringBuilder.toString();
    }

    @AfterAll
    public void destroyCluster() throws Exception {
        cluster.close();
    }

    private Properties adminProperties(boolean usingBootstrapControllers) {
        Properties properties = cluster.clientProperties();
        if (usingBootstrapControllers) {
            properties.remove(BOOTSTRAP_SERVERS_CONFIG);
            properties.setProperty(BOOTSTRAP_CONTROLLERS_CONFIG, bootstrapControllerString);
        }
        return properties;
    }

    @Test
    public void testPutBrokersInBootstrapControllersConfig() {
        Properties properties = cluster.clientProperties();
        properties.put(BOOTSTRAP_CONTROLLERS_CONFIG, properties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
        properties.remove(BOOTSTRAP_SERVERS_CONFIG);
        try (Admin admin = Admin.create(properties)) {
            ExecutionException exception = assertThrows(ExecutionException.class,
                () -> admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES));
            assertNotNull(exception.getCause());
            assertEquals(MismatchedEndpointTypeException.class, exception.getCause().getClass());
            assertEquals("The request was sent to an endpoint of type BROKER, but we wanted " +
                "an endpoint of type CONTROLLER", exception.getCause().getMessage());
        }
    }

    @Disabled
    @Test
    public void testPutControllersInBootstrapBrokersConfig() {
        Properties properties = cluster.clientProperties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapControllerString);
        try (Admin admin = Admin.create(properties)) {
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES));
            assertNotNull(exception.getCause());
            assertEquals(MismatchedEndpointTypeException.class, exception.getCause().getClass());
            assertEquals("This endpoint does not appear to be a BROKER.",
                    exception.getCause().getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDescribeCluster(boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminProperties(usingBootstrapControllers))) {
            DescribeClusterResult result = admin.describeCluster();
            assertEquals(cluster.controllers().values().iterator().next().clusterId(),
                    result.clusterId().get(1, TimeUnit.MINUTES));
            if (usingBootstrapControllers) {
                assertEquals(((QuorumController) cluster.waitForActiveController()).nodeId(),
                    result.controller().get().id());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDescribeFeatures(boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminProperties(usingBootstrapControllers))) {
            DescribeFeaturesResult result = admin.describeFeatures();
            short metadataVersion = cluster.controllers().values().iterator().next().
                featuresPublisher().features().metadataVersion().featureLevel();
            assertEquals(new FinalizedVersionRange(metadataVersion, metadataVersion),
                result.featureMetadata().get(1, TimeUnit.MINUTES).finalizedFeatures().
                    get(MetadataVersion.FEATURE_NAME));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testUpdateFeatures(boolean usingBootstrapControllers) {
        try (Admin admin = Admin.create(adminProperties(usingBootstrapControllers))) {
            UpdateFeaturesResult result = admin.updateFeatures(Collections.singletonMap("foo.bar.feature",
                new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE)),
                    new UpdateFeaturesOptions());
            ExecutionException exception =
                assertThrows(ExecutionException.class,
                    () -> result.all().get(1, TimeUnit.MINUTES));
            assertNotNull(exception.getCause());
            assertEquals(InvalidUpdateVersionException.class, exception.getCause().getClass());
            assertTrue(exception.getCause().getMessage().endsWith("does not support this feature."),
                "expected message to end with 'does not support this feature', but it was: " +
                    exception.getCause().getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDescribeMetadataQuorum(boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminProperties(usingBootstrapControllers))) {
            DescribeMetadataQuorumResult result = admin.describeMetadataQuorum();
            assertEquals(((QuorumController) cluster.waitForActiveController()).nodeId(),
                result.quorumInfo().get(1, TimeUnit.MINUTES).leaderId());
        }
    }

    @Test
    public void testUsingBootstrapControllersOnUnsupportedAdminApi() {
        try (Admin admin = Admin.create(adminProperties(true))) {
            ListOffsetsResult result = admin.listOffsets(Collections.singletonMap(
                    new TopicPartition("foo", 0), OffsetSpec.earliest()));
            ExecutionException exception =
                assertThrows(ExecutionException.class,
                    () -> result.all().get(1, TimeUnit.MINUTES));
            assertNotNull(exception.getCause());
            assertEquals(UnsupportedEndpointTypeException.class, exception.getCause().getClass());
            assertEquals("This Admin API is not yet supported when communicating directly with " +
                "the controller quorum.", exception.getCause().getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testIncrementalAlterConfigs(boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminProperties(usingBootstrapControllers))) {
            int nodeId = usingBootstrapControllers ?
                cluster.controllers().values().iterator().next().config().nodeId() :
                cluster.brokers().values().iterator().next().config().nodeId();
            ConfigResource nodeResource = new ConfigResource(BROKER, "" + nodeId);
            ConfigResource defaultResource = new ConfigResource(BROKER, "");
            Map<ConfigResource, Collection<AlterConfigOp>> alterations = new HashMap<>();
            alterations.put(nodeResource, Arrays.asList(
                new AlterConfigOp(new ConfigEntry("my.custom.config", "foo"),
                    AlterConfigOp.OpType.SET)));
            alterations.put(defaultResource, Arrays.asList(
                new AlterConfigOp(new ConfigEntry("my.custom.config", "bar"),
                    AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(alterations).all().get(1, TimeUnit.MINUTES);
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                Config config = admin.describeConfigs(Arrays.asList(nodeResource)).
                    all().get(1, TimeUnit.MINUTES).get(nodeResource);
                ConfigEntry entry = config.entries().stream().
                    filter(e -> e.name().equals("my.custom.config")).
                    findFirst().get();
                assertEquals(DYNAMIC_BROKER_CONFIG, entry.source(),
                    "Expected entry for my.custom.config to come from DYNAMIC_BROKER_CONFIG. " +
                    "Instead, the entry was: " + entry);
            });
        }
    }
}
