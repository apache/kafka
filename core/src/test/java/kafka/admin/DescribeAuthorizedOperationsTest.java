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

package kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.test.junit.ClusterTestExtensions;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.security.authorizer.AclEntry;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.acl.AccessControlEntryFilter.ANY;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ClusterTestExtensions.class)
public class DescribeAuthorizedOperationsTest {
    private static final String GROUP1 = "group1";
    private static final String GROUP2 = "group2";
    private static final String GROUP3 = "group3";
    private static final ResourcePattern GROUP1_PATTERN = new ResourcePattern(ResourceType.GROUP, GROUP1, PatternType.LITERAL);
    private static final ResourcePattern GROUP2_PATTERN = new ResourcePattern(ResourceType.GROUP, GROUP2, PatternType.LITERAL);
    private static final ResourcePattern GROUP3_PATTERN = new ResourcePattern(ResourceType.GROUP, GROUP3, PatternType.LITERAL);
    private static final ResourcePattern CLUSTER_PATTERN = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);
    private static final AccessControlEntry ALTER_ENTRY = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, ALTER);
    private static final AccessControlEntry DESCRIBE_ENTRY = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, DESCRIBE);

    static List<ClusterConfig> generator() {
        return List.of(
            ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setServerProperties(Map.of(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1"))
                .setServerProperties(Map.of(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1"))
                .setBrokerSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
                .setControllerSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
                .build()
        );
    }

    private static AccessControlEntry createAccessControlEntry(String username, AclOperation operation) {
        return new AccessControlEntry(
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username).toString(),
            AclEntry.WILDCARD_HOST,
            operation,
            ALLOW
        );
    }

    private Map<String, Object> createAdminConfig(String username, String password) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG,
            String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", username, password));
        return configs;
    }

    private void setupSecurity(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD))) {
            ResourcePattern topicResource = new ResourcePattern(ResourceType.TOPIC, AclEntry.WILDCARD_RESOURCE, PatternType.LITERAL);

            admin.createAcls(List.of(
                new AclBinding(CLUSTER_PATTERN, ALTER_ENTRY),
                new AclBinding(topicResource, DESCRIBE_ENTRY)
            )).all().get();

            clusterInstance.waitAcls(new AclBindingFilter(CLUSTER_PATTERN.toFilter(), ANY), Set.of(ALTER_ENTRY));
            clusterInstance.waitAcls(new AclBindingFilter(topicResource.toFilter(), ANY), Set.of(DESCRIBE_ENTRY));
        }
    }

    @ClusterTemplate("generator")
    public void testConsumerGroupAuthorizedOperations(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        setupSecurity(clusterInstance);
        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD));
             Admin user1 = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD));
        ) {
            admin.createTopics(List.of(new NewTopic("topic1", 1, (short) 1)));
            clusterInstance.waitForTopic("topic1", 1);

            // create consumers to avoid group not found error
            TopicPartition tp = new TopicPartition("topic1", 0);
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0);
            admin.alterConsumerGroupOffsets(GROUP1, Map.of(tp, offsetAndMetadata)).all().get();
            admin.alterConsumerGroupOffsets(GROUP2, Map.of(tp, offsetAndMetadata)).all().get();
            admin.alterConsumerGroupOffsets(GROUP3, Map.of(tp, offsetAndMetadata)).all().get();

            AccessControlEntry allOperationsEntry = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, ALL);
            AccessControlEntry describeEntry = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, DESCRIBE);
            AccessControlEntry deleteEntry = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, DELETE);
            user1.createAcls(List.of(
                new AclBinding(GROUP1_PATTERN, allOperationsEntry),
                new AclBinding(GROUP2_PATTERN, describeEntry),
                new AclBinding(GROUP3_PATTERN, deleteEntry)
            )).all();
            clusterInstance.waitAcls(new AclBindingFilter(GROUP1_PATTERN.toFilter(), ANY), Set.of(allOperationsEntry));
            clusterInstance.waitAcls(new AclBindingFilter(GROUP2_PATTERN.toFilter(), ANY), Set.of(describeEntry));
            clusterInstance.waitAcls(new AclBindingFilter(GROUP3_PATTERN.toFilter(), ANY), Set.of(deleteEntry));

            DescribeConsumerGroupsResult describeConsumerGroupsResult = user1.describeConsumerGroups(
                List.of(GROUP1, GROUP2, GROUP3), new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true));
            assertEquals(3, describeConsumerGroupsResult.describedGroups().size());

            ConsumerGroupDescription group1Description = describeConsumerGroupsResult.describedGroups().get(GROUP1).get();
            assertEquals(AclEntry.supportedOperations(ResourceType.GROUP), group1Description.authorizedOperations());

            ConsumerGroupDescription group2Description = describeConsumerGroupsResult.describedGroups().get(GROUP2).get();
            assertEquals(Set.of(DESCRIBE), group2Description.authorizedOperations());

            ConsumerGroupDescription group3Description = describeConsumerGroupsResult.describedGroups().get(GROUP3).get();
            assertEquals(Set.of(DESCRIBE, DELETE), group3Description.authorizedOperations());
        }
    }

    @ClusterTemplate("generator")
    public void testClusterAuthorizedOperations(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        setupSecurity(clusterInstance);
        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD))) {
            // test without includeAuthorizedOperations flag
            Set<AclOperation> authorizedOperations = admin.describeCluster().authorizedOperations().get();
            assertNull(authorizedOperations);

            // test with includeAuthorizedOperations flag
            authorizedOperations = admin.describeCluster(new DescribeClusterOptions().includeAuthorizedOperations(true)).authorizedOperations().get();
            assertEquals(Set.of(DESCRIBE, ALTER), authorizedOperations);
        }

        // enable all operations for cluster resource
        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD))) {
            AccessControlEntry allOperationEntry = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, ALL);
            admin.createAcls(List.of(new AclBinding(CLUSTER_PATTERN, allOperationEntry))).all().get();
            clusterInstance.waitAcls(
                new AclBindingFilter(CLUSTER_PATTERN.toFilter(), ANY),
                Set.of(allOperationEntry, ALTER_ENTRY)
            );
        }

        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD))) {
            Set<AclOperation> authorizedOperations = admin.describeCluster(new DescribeClusterOptions().includeAuthorizedOperations(true)).authorizedOperations().get();
            assertEquals(AclEntry.supportedOperations(ResourceType.CLUSTER), authorizedOperations);
        }
    }

    @ClusterTemplate("generator")
    public void testTopicAuthorizedOperations(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        String topic1 = "topic1";
        String topic2 = "topic2";
        setupSecurity(clusterInstance);
        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD))) {
            admin.createTopics(List.of(
                new NewTopic(topic1, 1, (short) 1),
                new NewTopic(topic2, 1, (short) 1)
            ));
            clusterInstance.waitForTopic(topic1, 1);
            clusterInstance.waitForTopic(topic2, 1);
        }

        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD))) {
            // test without includeAuthorizedOperations flag
            Map<String, TopicDescription> topicDescriptions = admin.describeTopics(List.of(topic1, topic2)).allTopicNames().get();
            assertNull(topicDescriptions.get(topic1).authorizedOperations());
            assertNull(topicDescriptions.get(topic2).authorizedOperations());

            // test with includeAuthorizedOperations flag
            topicDescriptions = admin.describeTopics(
                List.of(topic1, topic2),
                new DescribeTopicsOptions().includeAuthorizedOperations(true)).allTopicNames().get();
            assertEquals(Set.of(DESCRIBE), topicDescriptions.get(topic1).authorizedOperations());
            assertEquals(Set.of(DESCRIBE), topicDescriptions.get(topic2).authorizedOperations());
        }

        // add few permissions
        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD))) {
            ResourcePattern topic1Resource = new ResourcePattern(ResourceType.TOPIC, topic1, PatternType.LITERAL);
            ResourcePattern topic2Resource = new ResourcePattern(ResourceType.TOPIC, topic2, PatternType.LITERAL);
            AccessControlEntry allOperationEntry = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, ALL);
            AccessControlEntry deleteEntry = createAccessControlEntry(JaasUtils.KAFKA_PLAIN_USER1, DELETE);
            admin.createAcls(List.of(
                new AclBinding(topic1Resource, allOperationEntry),
                new AclBinding(topic2Resource, deleteEntry)
            )).all().get();
            clusterInstance.waitAcls(
                new AclBindingFilter(topic1Resource.toFilter(), ANY),
                Set.of(allOperationEntry)
            );
            clusterInstance.waitAcls(
                new AclBindingFilter(topic2Resource.toFilter(), ANY),
                Set.of(deleteEntry)
            );
        }

        try (Admin admin = clusterInstance.admin(createAdminConfig(JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD))) {
            Map<String, TopicDescription> topicDescriptions = admin.describeTopics(
                List.of(topic1, topic2),
                new DescribeTopicsOptions().includeAuthorizedOperations(true)).allTopicNames().get();
            assertEquals(AclEntry.supportedOperations(ResourceType.TOPIC), topicDescriptions.get(topic1).authorizedOperations());
            assertEquals(Set.of(DESCRIBE, DELETE), topicDescriptions.get(topic2).authorizedOperations());
        }
    }
}