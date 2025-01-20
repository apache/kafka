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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
@ExtendWith(ClusterTestExtensions.class)
@ClusterTestDefaults(types = {Type.KRAFT})
public class BootstrapControllersIntegrationTest {
    private Map<String, Object> adminConfig(ClusterInstance clusterInstance, boolean usingBootstrapControllers) {
        return usingBootstrapControllers ?
                Collections.singletonMap(BOOTSTRAP_CONTROLLERS_CONFIG, clusterInstance.bootstrapControllers()) :
                Collections.singletonMap(BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
    }

    @ClusterTest
    public void testPutBrokersInBootstrapControllersConfig(ClusterInstance clusterInstance) {
        Map<String, Object> config = Collections.singletonMap(BOOTSTRAP_CONTROLLERS_CONFIG, clusterInstance.bootstrapServers());
        try (Admin admin = Admin.create(config)) {
            ExecutionException exception = assertThrows(ExecutionException.class,
                () -> admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES));
            assertNotNull(exception.getCause());
            assertEquals(MismatchedEndpointTypeException.class, exception.getCause().getClass());
            assertEquals("The request was sent to an endpoint of type BROKER, but we wanted " +
                "an endpoint of type CONTROLLER", exception.getCause().getMessage());
        }
    }

    @ClusterTest
    public void testPutControllersInBootstrapBrokersConfig(ClusterInstance clusterInstance) {
        Map<String, Object> config = Collections.singletonMap(BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapControllers());
        try (Admin admin = Admin.create(config)) {
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES));
            assertNotNull(exception.getCause());
            assertEquals(UnsupportedVersionException.class, exception.getCause().getClass());
            assertEquals("The node does not support METADATA", exception.getCause().getMessage());
        }
    }

    @ClusterTest
    public void testDescribeClusterByControllers(ClusterInstance clusterInstance) throws Exception {
        testDescribeCluster(clusterInstance, true);
    }

    @ClusterTest
    public void testDescribeCluster(ClusterInstance clusterInstance) throws Exception {
        testDescribeCluster(clusterInstance, false);
    }

    private void testDescribeCluster(ClusterInstance clusterInstance, boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, usingBootstrapControllers))) {
            DescribeClusterResult result = admin.describeCluster();
            assertEquals(clusterInstance.clusterId(), result.clusterId().get(1, TimeUnit.MINUTES));
            if (usingBootstrapControllers) {
                assertTrue(clusterInstance.controllerIds().contains(result.controller().get().id()));
            }
        }
    }

    @ClusterTest
    public void testDescribeFeaturesByControllers(ClusterInstance clusterInstance) throws Exception {
        testDescribeFeatures(clusterInstance, true);
    }

    @ClusterTest
    public void testDescribeFeatures(ClusterInstance clusterInstance) throws Exception {
        testDescribeFeatures(clusterInstance, false);
    }

    private void testDescribeFeatures(ClusterInstance clusterInstance, boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, usingBootstrapControllers))) {
            DescribeFeaturesResult result = admin.describeFeatures();
            short metadataVersion = clusterInstance.config().metadataVersion().featureLevel();
            assertEquals(new FinalizedVersionRange(metadataVersion, metadataVersion),
                    result.featureMetadata().get(1, TimeUnit.MINUTES).finalizedFeatures().
                            get(MetadataVersion.FEATURE_NAME));
        }
    }

    @ClusterTest
    public void testUpdateFeaturesByControllers(ClusterInstance clusterInstance) {
        testUpdateFeatures(clusterInstance, true);
    }

    @ClusterTest
    public void testUpdateFeatures(ClusterInstance clusterInstance) {
        testUpdateFeatures(clusterInstance, false);
    }

    private void testUpdateFeatures(ClusterInstance clusterInstance, boolean usingBootstrapControllers) {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, usingBootstrapControllers))) {
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

    @ClusterTest
    public void testDescribeMetadataQuorumByControllers(ClusterInstance clusterInstance) throws Exception {
        testDescribeMetadataQuorum(clusterInstance, true);
    }

    @ClusterTest
    public void testDescribeMetadataQuorum(ClusterInstance clusterInstance) throws Exception {
        testDescribeMetadataQuorum(clusterInstance, false);
    }

    private void testDescribeMetadataQuorum(ClusterInstance clusterInstance, boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, usingBootstrapControllers))) {
            DescribeMetadataQuorumResult result = admin.describeMetadataQuorum();
            assertTrue(clusterInstance.controllerIds().contains(
                    result.quorumInfo().get(1, TimeUnit.MINUTES).leaderId()));
        }
    }

    @ClusterTest
    public void testUsingBootstrapControllersOnUnsupportedAdminApi(ClusterInstance clusterInstance) {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, true))) {
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

    @ClusterTest
    public void testIncrementalAlterConfigsByControllers(ClusterInstance clusterInstance) throws Exception {
        testIncrementalAlterConfigs(clusterInstance, true);
    }

    @ClusterTest
    public void testIncrementalAlterConfigs(ClusterInstance clusterInstance) throws Exception {
        testIncrementalAlterConfigs(clusterInstance, false);
    }

    private void testIncrementalAlterConfigs(ClusterInstance clusterInstance, boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, usingBootstrapControllers))) {
            int nodeId = usingBootstrapControllers ?
                    clusterInstance.controllers().values().iterator().next().config().nodeId() :
                    clusterInstance.brokers().values().iterator().next().config().nodeId();
            ConfigResource nodeResource = new ConfigResource(BROKER, "" + nodeId);
            ConfigResource defaultResource = new ConfigResource(BROKER, "");
            Map<ConfigResource, Collection<AlterConfigOp>> alterations = new HashMap<>();
            alterations.put(nodeResource, Collections.singletonList(
                    new AlterConfigOp(new ConfigEntry("my.custom.config", "foo"),
                            AlterConfigOp.OpType.SET)));
            alterations.put(defaultResource, Collections.singletonList(
                    new AlterConfigOp(new ConfigEntry("my.custom.config", "bar"),
                            AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(alterations).all().get(1, TimeUnit.MINUTES);
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                Config config = admin.describeConfigs(Collections.singletonList(nodeResource)).
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

    @ClusterTest(brokers = 3)
    public void testAlterReassignmentsWithBootstrapControllers(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        String topicName = "foo";
        try (Admin admin = Admin.create(adminConfig(clusterInstance, false))) {
            Map<Integer, List<Integer>> assignments = new HashMap<>();
            assignments.put(0, Arrays.asList(0, 1, 2));
            assignments.put(1, Arrays.asList(1, 2, 0));
            assignments.put(2, Arrays.asList(2, 1, 0));
            CreateTopicsResult createTopicResult = admin.createTopics(Collections.singletonList(new NewTopic(topicName, assignments)));
            createTopicResult.all().get();
            waitForTopics(admin, Collections.singleton(topicName));

            List<Integer> part0Reassignment = Arrays.asList(2, 1, 0);
            List<Integer> part1Reassignment = Arrays.asList(0, 1, 2);
            List<Integer> part2Reassignment = Arrays.asList(1, 2);
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
            reassignments.put(new TopicPartition(topicName, 0), Optional.of(new NewPartitionReassignment(part0Reassignment)));
            reassignments.put(new TopicPartition(topicName, 1), Optional.of(new NewPartitionReassignment(part1Reassignment)));
            reassignments.put(new TopicPartition(topicName, 2), Optional.of(new NewPartitionReassignment(part2Reassignment)));

            try (Admin adminWithBootstrapControllers = Admin.create(adminConfig(clusterInstance, true))) {
                adminWithBootstrapControllers.alterPartitionReassignments(reassignments).all().get();
                TestUtils.waitForCondition(
                        () -> adminWithBootstrapControllers.listPartitionReassignments().reassignments().get().isEmpty(),
                        "The reassignment never completed.");
            }

            List<List<Integer>> expectedMapping = Arrays.asList(part0Reassignment, part1Reassignment, part2Reassignment);
            TestUtils.waitForCondition(() -> {
                Map<String, TopicDescription> topicInfoMap = admin.describeTopics(Collections.singleton(topicName)).allTopicNames().get();
                if (topicInfoMap.containsKey(topicName)) {
                    List<List<Integer>> currentMapping = translatePartitionInfoToNodeIdList(topicInfoMap.get(topicName).partitions());
                    return expectedMapping.equals(currentMapping);
                } else {
                    return false;
                }
            }, "Timed out waiting for replica assignments for topic " + topicName);
        }
    }

    private static void waitForTopics(Admin admin, Set<String> expectedTopics) throws InterruptedException {
        TestUtils.waitForCondition(() -> admin.listTopics().names().get().containsAll(expectedTopics),
                "timed out waiting for topics");
    }

    private static List<List<Integer>> translatePartitionInfoToNodeIdList(List<TopicPartitionInfo> partitions) {
        return partitions.stream()
                .map(partition -> partition.replicas().stream().map(Node::id).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
        @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    })
    public void testAclsByControllers(ClusterInstance clusterInstance) throws Exception {
        testAcls(clusterInstance, true);
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
        @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    })
    public void testAcls(ClusterInstance clusterInstance) throws Exception {
        testAcls(clusterInstance, false);
    }

    private void testAcls(ClusterInstance clusterInstance, boolean usingBootstrapControllers) throws Exception {
        try (Admin admin = Admin.create(adminConfig(clusterInstance, usingBootstrapControllers))) {
            ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL);
            AccessControlEntry accessControlEntry = new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
            AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
            assertDoesNotThrow(() -> admin.createAcls(Collections.singleton(aclBinding)).all().get(1, TimeUnit.MINUTES));

            clusterInstance.waitAcls(new AclBindingFilter(resourcePattern.toFilter(), AccessControlEntryFilter.ANY),
                    Collections.singleton(accessControlEntry));

            Collection<AclBinding> aclBindings = admin.describeAcls(AclBindingFilter.ANY).values().get(1, TimeUnit.MINUTES);
            assertEquals(1, aclBindings.size());
            assertEquals(aclBinding, aclBindings.iterator().next());

            Collection<AclBinding> deletedAclBindings = admin.deleteAcls(Collections.singleton(AclBindingFilter.ANY)).all().get(1, TimeUnit.MINUTES);
            assertEquals(1, deletedAclBindings.size());
            assertEquals(aclBinding, deletedAclBindings.iterator().next());
        }
    }
}
