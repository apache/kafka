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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.mirror.DefaultConfigPropertyFilter;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.clients.admin.FakeForwardingAdminWithLocalMetadata;
import org.apache.kafka.connect.mirror.clients.admin.FakeLocalMetadataStore;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.connect.util.clusters.UngracefulShutdownException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.FORWARDING_ADMIN_CLASS;
import static org.apache.kafka.connect.mirror.TestUtils.generateRecords;
import static org.apache.kafka.connect.mirror.integration.MirrorConnectorsIntegrationBaseTest.basicMM2Config;
import static org.apache.kafka.connect.mirror.integration.MirrorConnectorsIntegrationBaseTest.waitUntilMirrorMakerIsRunning;
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
public class MirrorConnectorsWithCustomForwardingAdminIntegrationTest {
    protected static final int NUM_RECORDS_PER_PARTITION = 1;
    protected static final int NUM_PARTITIONS = 1;
    protected static final int NUM_RECORDS_PRODUCED = NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION;
    protected static final Duration CONSUMER_POLL_TIMEOUT_MS = Duration.ofMillis(500L);
    protected static final String PRIMARY_CLUSTER_ALIAS = "primary";
    protected static final String BACKUP_CLUSTER_ALIAS = "backup";
    protected static final List<Class<? extends Connector>> CONNECTOR_LIST = Arrays.asList(
            MirrorSourceConnector.class,
            MirrorCheckpointConnector.class,
            MirrorHeartbeatConnector.class);
    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsWithCustomForwardingAdminIntegrationTest.class);

    private static final int TOPIC_SYNC_DURATION_MS = 60_000;
    private static final int REQUEST_TIMEOUT_DURATION_MS = 60_000;
    private static final int NUM_WORKERS = 1;
    protected Map<String, String> mm2Props = new HashMap<>();
    protected MirrorMakerConfig mm2Config;
    protected EmbeddedConnectCluster primary;
    protected EmbeddedConnectCluster backup;
    protected Exit.Procedure exitProcedure;
    protected Properties primaryBrokerProps = new Properties();
    protected Properties backupBrokerProps = new Properties();
    protected Map<String, String> primaryWorkerProps = new HashMap<>();
    protected Map<String, String> backupWorkerProps = new HashMap<>();
    private volatile boolean shuttingDown;
    private Exit.Procedure haltProcedure;

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
    protected static Map<String, String> connectorUserConfig() {
        Map<String, String> superUserClientConfig = new HashMap<>();
        superUserClientConfig.put("sasl.mechanism", "PLAIN");
        superUserClientConfig.put("security.protocol", "SASL_PLAINTEXT");
        superUserClientConfig.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"connector\" "
                        + "password=\"connector_pwd\";");
        return superUserClientConfig;
    }

    /*
     * wait for the topic created on the cluster
     */
    protected static void waitForTopicCreated(EmbeddedConnectCluster cluster, String topicName) throws InterruptedException {
        try (final Admin adminClient = cluster.kafka().createAdminClient(Utils.mkProperties(superUserConfig()))) {
            waitForCondition(() -> adminClient.listTopics().names().get().contains(topicName), TOPIC_SYNC_DURATION_MS,
                    "Topic: " + topicName + " didn't get created on cluster: " + cluster.getName()
            );
        }
    }

    /*
     * wait for the topic created on the cluster
     */
    protected static void waitForTopicPartitionCreated(EmbeddedConnectCluster cluster, String topicName, int totalNumPartitions) throws InterruptedException {
        try (final Admin adminClient = cluster.kafka().createAdminClient(Utils.mkProperties(superUserConfig()))) {
            waitForCondition(() -> adminClient.describeTopics(Collections.singleton(topicName)).allTopicNames().get()
                            .get(topicName).partitions().size() == totalNumPartitions, TOPIC_SYNC_DURATION_MS,
                    "Topic: " + topicName + "'s partitions didn't get created on cluster: " + cluster.getName()
            );
        }
    }

    /*
     * delete all topics and their ACLs of the input kafka cluster
     */
    private static void deleteAllTopicsWithTheirACLs(EmbeddedKafkaCluster cluster) throws Exception {
        try (final Admin adminClient = cluster.createAdminClient(Utils.mkProperties(superUserConfig()))) {
            Set<String> topicsToBeDeleted = adminClient.listTopics().names().get();
            log.debug("Deleting topics: {} ", topicsToBeDeleted);
            adminClient.deleteTopics(topicsToBeDeleted).all().get();
            List<AclBindingFilter> aclBindingFilters = topicsToBeDeleted.stream().map(topic -> new AclBindingFilter(
                            new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.ANY),
                            AccessControlEntryFilter.ANY
                    )
            ).collect(Collectors.toList());
            adminClient.deleteAcls(aclBindingFilters);
        }
    }

    /*
     * retrieve the config value based on the input cluster, topic and config name
     */
    protected static String getTopicConfig(EmbeddedKafkaCluster cluster, String topic, String configName) throws Exception {
        try (Admin client = cluster.createAdminClient(Utils.mkProperties(superUserConfig()))) {
            Collection<ConfigResource> cr = Collections.singleton(
                    new ConfigResource(ConfigResource.Type.TOPIC, topic));

            DescribeConfigsResult configsResult = client.describeConfigs(cr);
            Config allConfigs = (Config) configsResult.all().get().values().toArray()[0];
            return allConfigs.get(configName).value();
        }
    }

    /*
     * retrieve the acl details based on the input cluster for given topic name.
     */
    protected static Collection<AclBinding> getAclBindings(EmbeddedKafkaCluster cluster, String topic) throws Exception {
        try (Admin client = cluster.createAdminClient(Utils.mkProperties(superUserConfig()))) {
            ResourcePatternFilter topicFilter = new ResourcePatternFilter(ResourceType.TOPIC,
                    topic, PatternType.ANY);
            return client.describeAcls(new AclBindingFilter(topicFilter, AccessControlEntryFilter.ANY)).values().get();
        }
    }

    @BeforeEach
    public void startClusters() throws Exception {
        HashMap<String, String> additionalConfig = new HashMap<String, String>() {{
                put("topics", "test-topic-.*, primary.test-topic-.*, backup.test-topic-.*");
                put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
                put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "true");
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

        primary.kafka().createAdminClient(Utils.mkProperties(superUserConfig())).createAcls(Arrays.asList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
                        new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                )
        )).all().get();
        backup.kafka().createAdminClient(Utils.mkProperties(superUserConfig())).createAcls(Arrays.asList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
                        new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                )
        )).all().get();
    }
    public void startClusters(Map<String, String> additionalMM2Config) throws Exception {
        shuttingDown = false;
        exitProcedure = (code, message) -> {
            if (shuttingDown) {
                // ignore this since we're shutting down Connect and Kafka and timing isn't always great
                return;
            }
            if (code != 0) {
                String exitMessage = "Abrupt service exit with code " + code + " and message " + message;
                log.warn(exitMessage);
                throw new UngracefulShutdownException(exitMessage);
            }
        };
        haltProcedure = (code, message) -> {
            if (shuttingDown) {
                // ignore this since we're shutting down Connect and Kafka and timing isn't always great
                return;
            }
            if (code != 0) {
                String haltMessage = "Abrupt service halt with code " + code + " and message " + message;
                log.warn(haltMessage);
                throw new UngracefulShutdownException(haltMessage);
            }
        };
        // Override the exit and halt procedure that Connect and Kafka will use. For these integration tests,
        // we don't want to exit the JVM and instead simply want to fail the test
        Exit.setExitProcedure(exitProcedure);
        Exit.setHaltProcedure(haltProcedure);

        primaryBrokerProps.put("auto.create.topics.enable", "false");
        backupBrokerProps.put("auto.create.topics.enable", "false");
        enableAclAuthorizer(primaryBrokerProps);
        enableAclAuthorizer(backupBrokerProps);
        primaryWorkerProps.putAll(superUserConfig());
        backupWorkerProps.putAll(superUserConfig());


        mm2Props.putAll(basicMM2Config());
        mm2Props.put("max.tasks", "1");
        mm2Props.putAll(superUserConfig());
        mm2Props.putAll(additionalMM2Config);

        // exclude topic config:
        mm2Props.put(DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG, "delete\\.retention\\..*");

        mm2Config = new MirrorMakerConfig(mm2Props);
        primaryWorkerProps = mm2Config.workerConfig(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
        backupWorkerProps.putAll(mm2Config.workerConfig(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS)));

        primary = new EmbeddedConnectCluster.Builder()
                .name(PRIMARY_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .brokerProps(primaryBrokerProps)
                .workerProps(primaryWorkerProps)
                .maskExitProcedures(false)
                .build();

        backup = new EmbeddedConnectCluster.Builder()
                .name(BACKUP_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .brokerProps(backupBrokerProps)
                .workerProps(backupWorkerProps)
                .maskExitProcedures(false)
                .build();

        primary.start();
        primary.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Workers of " + PRIMARY_CLUSTER_ALIAS + "-connect-cluster did not start in time.");

        waitForTopicCreated(primary, "mm2-status.backup.internal");
        waitForTopicCreated(primary, "mm2-offsets.backup.internal");
        waitForTopicCreated(primary, "mm2-configs.backup.internal");

        backup.start();
        backup.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Workers of " + BACKUP_CLUSTER_ALIAS + "-connect-cluster did not start in time.");

        waitForTopicCreated(backup, "mm2-status.primary.internal");
        waitForTopicCreated(backup, "mm2-offsets.primary.internal");
        waitForTopicCreated(backup, "mm2-configs.primary.internal");

        createTopics();

        Map<String, Object> consumerProps = new HashMap<>(superUserConfig());
        consumerProps.put("group.id", "consumer-group-dummy");
        warmUpConsumer(consumerProps);

        log.info(PRIMARY_CLUSTER_ALIAS + " REST service: {}", primary.endpointForResource("connectors"));
        log.info(BACKUP_CLUSTER_ALIAS + " REST service: {}", backup.endpointForResource("connectors"));
        log.info(PRIMARY_CLUSTER_ALIAS + " brokers: {}", primary.kafka().bootstrapServers());
        log.info(BACKUP_CLUSTER_ALIAS + " brokers: {}", backup.kafka().bootstrapServers());

        // now that the brokers are running, we can finish setting up the Connectors
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + ".bootstrap.servers", primary.kafka().bootstrapServers());
        mm2Props.put(BACKUP_CLUSTER_ALIAS + ".bootstrap.servers", backup.kafka().bootstrapServers());
    }

    @AfterEach
    public void shutdownClusters() throws Exception {
        try {
            for (String x : primary.connectors()) {
                primary.deleteConnector(x);
            }
            for (String x : backup.connectors()) {
                backup.deleteConnector(x);
            }
            deleteAllTopicsWithTheirACLs(primary.kafka());
            deleteAllTopicsWithTheirACLs(backup.kafka());
        } finally {
            shuttingDown = true;
            try {
                try {
                    primary.stop();
                } finally {
                    backup.stop();
                }
            } finally {
                Exit.resetExitProcedure();
                Exit.resetHaltProcedure();
            }
        }
    }

    @Test
    public void testReplicationIsCreatingTopicsUsingProvidedForwardingAdmin() throws Exception {
        produceMessages(primary, "test-topic-1");
        produceMessages(backup, "test-topic-1");

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

        // expect to use FakeForwardingAdminWithLocalMetadata to create remote topics into local store
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("primary.test-topic-1"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("backup.test-topic-1"));

        // expect to use FakeForwardingAdminWithLocalMetadata to create internal topics into local store
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("heartbeats"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("backup.heartbeats"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("primary.heartbeats"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("backup.checkpoints.internal"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("primary.checkpoints.internal"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("mm2-offset-syncs.primary.internal"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("mm2-offset-syncs.backup.internal"));
    }

    @Test
    public void testCreatePartitionsUseProvidedForwardingAdmin() throws Exception {
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");

        // expect to use FakeForwardingAdminWithLocalMetadata to create remote topics into local store
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("primary.test-topic-1"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("backup.test-topic-1"));
        assertEquals(String.valueOf(NUM_PARTITIONS), FakeLocalMetadataStore.allTopics.get("primary.test-topic-1").get("partitions"));
        assertEquals(String.valueOf(NUM_PARTITIONS), FakeLocalMetadataStore.allTopics.get("backup.test-topic-1").get("partitions"));

        // increase number of partitions
        Map<String, NewPartitions> newPartitions = Collections.singletonMap("test-topic-1", NewPartitions.increaseTo(NUM_PARTITIONS + 1));

        primary.kafka().createAdminClient(Utils.mkProperties(superUserConfig())).createPartitions(newPartitions).all().get();
        waitForTopicPartitionCreated(backup, "primary.test-topic-1", NUM_PARTITIONS + 1);

        // expect to use FakeForwardingAdminWithLocalMetadata to update number of partitions in local store
        assertEquals(String.valueOf(NUM_PARTITIONS + 1), FakeLocalMetadataStore.allTopics.get("primary.test-topic-1").get("partitions"));
    }

    @Test
    public void testSyncTopicConfigUseProvidedForwardingAdmin() throws Exception {
        mm2Props.put("sync.topic.configs.enabled", "true");
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");

        // expect to use FakeForwardingAdminWithLocalMetadata to create remote topics into local store
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("primary.test-topic-1"));
        assertTrue(FakeLocalMetadataStore.allTopics.containsKey("backup.test-topic-1"));

        // expect to use FakeForwardingAdminWithLocalMetadata to update topic config in local store
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, getTopicConfig(backup.kafka(), "primary.test-topic-1", TopicConfig.CLEANUP_POLICY_CONFIG),
                "topic config was synced");
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, FakeLocalMetadataStore.allTopics.get("primary.test-topic-1").get(TopicConfig.CLEANUP_POLICY_CONFIG));
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
        primary.kafka().createAdminClient(Utils.mkProperties(superUserConfig())).createAcls(aclBindings).all().get();
        backup.kafka().createAdminClient(Utils.mkProperties(superUserConfig())).createAcls(aclBindings).all().get();

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
        assertTrue(FakeLocalMetadataStore.allAcls.get("User:dummy").containsAll(Arrays.asList(expectedACLOnBackupCluster, expectedACLOnPrimaryCluster)));
    }

    /*
     *  produce messages to the cluster and topic
     */
    protected void produceMessages(EmbeddedConnectCluster cluster, String topicName) {
        Map<String, String> recordSent = generateRecords(NUM_RECORDS_PRODUCED);

        for (Map.Entry<String, String> entry : recordSent.entrySet()) {
            Map<String, Object> producerProps = new HashMap<>(superUserConfig());
            KafkaProducer<byte[], byte[]>  producer = cluster.kafka().createProducer(producerProps);

            long defaultProduceSendDuration = TimeUnit.SECONDS.toMillis(120);
            ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topicName, 0, entry.getKey().getBytes(), entry.getValue().getBytes());

            try {
                producer.send(msg).get(defaultProduceSendDuration, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new KafkaException("Could not produce message: " + msg, e);
            }
        }
    }


    private void createTopics() {
        // to verify topic config will be sync-ed across clusters
        Map<String, String> topicConfig = Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        Map<String, String> emptyMap = Collections.emptyMap();

        // increase admin client request timeout value to make the tests reliable.
        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS);
        adminClientConfig.putAll(superUserConfig());

        // create these topics before starting the connectors so we don't need to wait for discovery
        primary.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, topicConfig, adminClientConfig);
        backup.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, emptyMap, adminClientConfig);
    }

    /*
     * Generate some consumer activity on both clusters to ensure the checkpoint connector always starts promptly
     */
    protected void warmUpConsumer(Map<String, Object> consumerProps) {
        Consumer<byte[], byte[]> dummyConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1");
        dummyConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        dummyConsumer.commitSync();
        dummyConsumer.close();
        dummyConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1");
        dummyConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        dummyConsumer.commitSync();
        dummyConsumer.close();
    }
}
