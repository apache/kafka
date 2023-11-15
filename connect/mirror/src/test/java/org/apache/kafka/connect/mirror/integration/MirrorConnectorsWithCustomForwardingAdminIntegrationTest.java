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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.clients.admin.FakeForwardingAdminWithLocalMetadata;
import org.apache.kafka.connect.mirror.clients.admin.FakeLocalMetadataStore;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.FORWARDING_ADMIN_CLASS;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests MM2 is using provided ForwardingAdmin to create/alter topics, partitions and ACLs.
 */
@Tag("integration")
public class MirrorConnectorsWithCustomForwardingAdminIntegrationTest extends MirrorConnectorsIntegrationBaseTest {
    private static final int FAKE_LOCAL_METADATA_STORE_SYNC_DURATION_MS = 60_000;

    /*
     * enable ACL on brokers.
     */
    protected static void enableAclAuthorizer(Properties brokerProps) {
        brokerProps.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
        brokerProps.put("sasl.enabled.mechanisms", "PLAIN");
        brokerProps.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
        brokerProps.put("security.inter.broker.protocol", "SASL_PLAINTEXT");
        brokerProps.put("listeners", "SASL_PLAINTEXT://localhost:0");
        brokerProps.put("listener.name.sasl_plaintext.plain.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"super\" "
                        + "password=\"super_pwd\" "
                        + "user_connector=\"connector_pwd\" "
                        + "user_super=\"super_pwd\";");
        brokerProps.put("super.users", "User:super");
    }

    /*
     * return superUser auth config.
     */
    protected static Map<String, String> superUserConfig() {
        Map<String, String> superUserClientConfig = new HashMap<>();
        superUserClientConfig.put("sasl.mechanism", "PLAIN");
        superUserClientConfig.put("security.protocol", "SASL_PLAINTEXT");
        superUserClientConfig.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"super\" "
                        + "password=\"super_pwd\";");
        return superUserClientConfig;
    }

    /*
     * return connect user auth config.
     */
    protected static Map<String, String> connectorUserConfig() {
        Map<String, String> connectUserClientConfig = new HashMap<>();
        connectUserClientConfig.put("sasl.mechanism", "PLAIN");
        connectUserClientConfig.put("security.protocol", "SASL_PLAINTEXT");
        connectUserClientConfig.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"connector\" "
                        + "password=\"connector_pwd\";");
        return connectUserClientConfig;
    }

    /*
     * delete all acls from the input kafka cluster
     */
    private static void deleteAllACLs(EmbeddedKafkaCluster cluster) throws Exception {
        try (final Admin adminClient = cluster.createAdminClient()) {
            Set<String> topicsToBeDeleted = adminClient.listTopics().names().get();
            List<AclBindingFilter> aclBindingFilters = topicsToBeDeleted.stream().map(topic -> new AclBindingFilter(
                            new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.ANY),
                            AccessControlEntryFilter.ANY
                    )
            ).collect(Collectors.toList());
            adminClient.deleteAcls(aclBindingFilters);
        }
    }

    /*
     * retrieve the acl details based on the input cluster for given topic name.
     */
    protected static Collection<AclBinding> getAclBindings(EmbeddedKafkaCluster cluster, String topic) throws Exception {
        try (Admin client = cluster.createAdminClient()) {
            ResourcePatternFilter topicFilter = new ResourcePatternFilter(ResourceType.TOPIC,
                    topic, PatternType.ANY);
            return client.describeAcls(new AclBindingFilter(topicFilter, AccessControlEntryFilter.ANY)).values().get();
        }
    }

    @BeforeEach
    public void startClusters() throws Exception {
        enableAclAuthorizer(primaryBrokerProps);
        additionalPrimaryClusterClientsConfigs.putAll(superUserConfig());
        primaryWorkerProps.putAll(superUserConfig());

        enableAclAuthorizer(backupBrokerProps);
        additionalBackupClusterClientsConfigs.putAll(superUserConfig());
        backupWorkerProps.putAll(superUserConfig());

        HashMap<String, String> additionalConfig = new HashMap<String, String>(superUserConfig()) {{
                put(FORWARDING_ADMIN_CLASS, FakeForwardingAdminWithLocalMetadata.class.getName());
            }};

        superUserConfig().forEach((property, value) -> {
            additionalConfig.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + property, value);
            additionalConfig.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + property, value);
            additionalConfig.put("consumer." + property, value);
            additionalConfig.put("producer." + property, value);
        });

        connectorUserConfig().forEach((property, value) -> {
            additionalConfig.put("admin." + property, value);
            additionalConfig.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + property, value);
        });

        startClusters(additionalConfig);

        primary.kafka().createAdminClient().createAcls(Arrays.asList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
                        new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                )
        )).all().get();
        backup.kafka().createAdminClient().createAcls(Arrays.asList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
                        new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                )
        )).all().get();
    }

    @AfterEach
    public void shutdownClusters() throws Exception {
        deleteAllACLs(primary.kafka());
        deleteAllACLs(backup.kafka());
        FakeLocalMetadataStore.clear();
        super.shutdownClusters();
    }

    @Test
    public void testReplicationIsCreatingTopicsUsingProvidedForwardingAdmin() throws Exception {
        produceMessages(primaryProducer, "test-topic-1");
        produceMessages(backupProducer, "test-topic-1");
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");
        waitForTopicCreated(primary, "mm2-offset-syncs.backup.internal");
        waitForTopicCreated(primary, "backup.checkpoints.internal");
        waitForTopicCreated(primary, "backup.heartbeats");
        waitForTopicCreated(backup, "mm2-offset-syncs.primary.internal");
        waitForTopicCreated(backup, "primary.checkpoints.internal");
        waitForTopicCreated(backup, "primary.heartbeats");

        // expect to use FakeForwardingAdminWithLocalMetadata to create remote topics and internal topics into local store
        waitForTopicToPersistInFakeLocalMetadataStore("backup.test-topic-1");
        waitForTopicToPersistInFakeLocalMetadataStore("primary.test-topic-1");
        waitForTopicToPersistInFakeLocalMetadataStore("mm2-offset-syncs.backup.internal");
        waitForTopicToPersistInFakeLocalMetadataStore("backup.checkpoints.internal");
        waitForTopicToPersistInFakeLocalMetadataStore("backup.heartbeats");
        waitForTopicToPersistInFakeLocalMetadataStore("mm2-offset-syncs.primary.internal");
        waitForTopicToPersistInFakeLocalMetadataStore("primary.checkpoints.internal");
        waitForTopicToPersistInFakeLocalMetadataStore("primary.heartbeats");
        waitForTopicToPersistInFakeLocalMetadataStore("heartbeats");
    }

    @Test
    public void testCreatePartitionsUseProvidedForwardingAdmin() throws Exception {
        mm2Config = new MirrorMakerConfig(mm2Props);
        produceMessages(backupProducer, "test-topic-1");
        produceMessages(primaryProducer, "test-topic-1");
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");

        // make sure to remote topics are created using FakeForwardingAdminWithLocalMetadata
        waitForTopicToPersistInFakeLocalMetadataStore("backup.test-topic-1");
        waitForTopicToPersistInFakeLocalMetadataStore("primary.test-topic-1");

        // increase number of partitions
        Map<String, NewPartitions> newPartitions = Collections.singletonMap("test-topic-1", NewPartitions.increaseTo(NUM_PARTITIONS + 1));
        primary.kafka().createAdminClient().createPartitions(newPartitions).all().get();

        // make sure partition is created in the other cluster
        waitForTopicPartitionCreated(backup, "primary.test-topic-1", NUM_PARTITIONS + 1);

        // expect to use FakeForwardingAdminWithLocalMetadata to update number of partitions in local store
        waitForTopicConfigPersistInFakeLocalMetaDataStore("primary.test-topic-1", "partitions", String.valueOf(NUM_PARTITIONS + 1));
    }

    @Test
    public void testSyncTopicConfigUseProvidedForwardingAdmin() throws Exception {
        mm2Props.put("sync.topic.configs.enabled", "true");
        mm2Config = new MirrorMakerConfig(mm2Props);
        produceMessages(backupProducer, "test-topic-1");
        produceMessages(primaryProducer, "test-topic-1");
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");

        // make sure the topic config is synced into the other cluster
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, getTopicConfig(backup.kafka(), "primary.test-topic-1", TopicConfig.CLEANUP_POLICY_CONFIG),
            "topic config was synced");

        // expect to use FakeForwardingAdminWithLocalMetadata to create remote topics into local store
        waitForTopicToPersistInFakeLocalMetadataStore("backup.test-topic-1");
        waitForTopicToPersistInFakeLocalMetadataStore("primary.test-topic-1");

        // expect to use FakeForwardingAdminWithLocalMetadata to update topic config in local store
        waitForTopicConfigPersistInFakeLocalMetaDataStore("primary.test-topic-1", TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    }

    @Test
    public void testSyncTopicACLsUseProvidedForwardingAdmin() throws Exception {
        mm2Props.put("sync.topic.acls.enabled", "true");
        mm2Config = new MirrorMakerConfig(mm2Props);
        List<AclBinding> aclBindings = Arrays.asList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "test-topic-1", PatternType.LITERAL),
                        new AccessControlEntry("User:dummy", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
                )
        );
        primary.kafka().createAdminClient().createAcls(aclBindings).all().get();
        backup.kafka().createAdminClient().createAcls(aclBindings).all().get();

        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");

        // expect to use FakeForwardingAdminWithLocalMetadata to update topic ACLs on remote cluster
        AclBinding expectedACLOnBackupCluster = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "primary.test-topic-1", PatternType.LITERAL),
                new AccessControlEntry("User:dummy", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        );
        AclBinding expectedACLOnPrimaryCluster = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "backup.test-topic-1", PatternType.LITERAL),
                new AccessControlEntry("User:dummy", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        );

        assertTrue(getAclBindings(backup.kafka(), "primary.test-topic-1").contains(expectedACLOnBackupCluster), "topic ACLs was synced");
        assertTrue(getAclBindings(primary.kafka(), "backup.test-topic-1").contains(expectedACLOnPrimaryCluster), "topic ACLs was synced");

        // expect to use FakeForwardingAdminWithLocalMetadata to update topic ACLs in FakeLocalMetadataStore.allAcls
        assertTrue(FakeLocalMetadataStore.aclBindings("dummy").containsAll(Arrays.asList(expectedACLOnBackupCluster, expectedACLOnPrimaryCluster)));
    }

    void waitForTopicToPersistInFakeLocalMetadataStore(String topicName) throws InterruptedException {
        waitForCondition(() -> {
                return FakeLocalMetadataStore.containsTopic(topicName);
            }, FAKE_LOCAL_METADATA_STORE_SYNC_DURATION_MS,
            "Topic: " + topicName + " didn't get created in the FakeLocalMetadataStore"
        );
    }

    void waitForTopicConfigPersistInFakeLocalMetaDataStore(String topicName, String configName, String expectedConfigValue) throws InterruptedException {
        waitForCondition(() -> {
            return FakeLocalMetadataStore.topicConfig(topicName).getOrDefault(configName, "").equals(expectedConfigValue);
            }, FAKE_LOCAL_METADATA_STORE_SYNC_DURATION_MS,
            "Topic: " + topicName + "'s configs don't have " + configName + ":" + expectedConfigValue
        );
    }
}
