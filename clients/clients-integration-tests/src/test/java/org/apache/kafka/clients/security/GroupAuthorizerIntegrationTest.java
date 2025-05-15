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
package org.apache.kafka.clients.security;


import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(types = {Type.CO_KRAFT},
    serverProperties = {
//        @ClusterConfigProperty(key = ServerConfigs.BROKER_ID_CONFIG, value = "0"),
//        @ClusterConfigProperty(key = KRaftConfigs.NODE_ID_CONFIG, value = "0"),
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "Group:broker"),
//        @ClusterConfigProperty(key = "inter.broker.listener.name", value = "BROKER"),
//        @ClusterConfigProperty(key = SocketServerConfigs.LISTENERS_CONFIG, value = "BROKER://localhost:0,CLIENT://localhost:0,CONTROLLER://localhost:0"),
//        @ClusterConfigProperty(key = SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, value = "BROKER:PLAINTEXT,CLIENT:PLAINTEXT,CONTROLLER:PLAINTEXT"),
//        @ClusterConfigProperty(key = KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, value = "CONTROLLER"),
//        @ClusterConfigProperty(key = SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, value = "BROKER://localhost:0,CLIENT://localhost:0,CONTROLLER://localhost:0"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
        @ClusterConfigProperty(key = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, value = "org.apache.kafka.clients.security.GroupAuthorizerIntegrationTest$GroupPrincipalBuilder"),
        @ClusterConfigProperty(key = KRaftConfigs.PROCESS_ROLES_CONFIG, value = "broker,controller"),
        @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
    })
public class GroupAuthorizerIntegrationTest {
    private static final KafkaPrincipal BROKER_PRINCIPAL = new KafkaPrincipal("Group", "broker");
    private static final KafkaPrincipal CLIENT_PRINCIPAL = new KafkaPrincipal("Group", "client");

    private static final String BROKER_LISTENER_NAME = "BROKER";
    private static final String CLIENT_LISTENER_NAME = "CLIENT";
    private static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";

    private static final ListenerName INTER_BROKER_LISTENER_NAME = new ListenerName(BROKER_LISTENER_NAME);
    private static final ListenerName LISTENER_NAME = new ListenerName(CLIENT_LISTENER_NAME);

    private static final String GROUP_PRINCIPAL_BUILDER_CLASS_NAME = GroupPrincipalBuilder.class.getName();


    private Authorizer getAuthorizer(ClusterInstance clusterInstance) {
        return clusterInstance.controllers().values().stream()
                .filter(server -> server.authorizerPlugin().isDefined())
                .map(server -> server.authorizerPlugin().get().get()).toList().get(0);
    }

    private void setup(ClusterInstance clusterInstance) throws InterruptedException {

        // Allow inter-broker communication
        addAndVerifyAcls(
                Collections.singleton(createAcl(AclOperation.CLUSTER_ACTION, AclPermissionType.ALLOW, BROKER_PRINCIPAL)),
                new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
                clusterInstance
        );

        addAndVerifyAcls(
                Collections.singleton(createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME, PatternType.LITERAL),
                clusterInstance
        );

        KafkaBroker broker = clusterInstance.brokers().values().stream().toList().get(0);

        int partitions = broker.config().getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG);
        short replicationFactor = broker.config().getShort(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG);
        NewTopic offsetTopic = new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, partitions, replicationFactor);

        Map<String, Object> configs = new HashMap<>(2);
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        try (Admin admin = clusterInstance.admin(configs)) {
            admin.createTopics(Collections.singleton(offsetTopic));
            clusterInstance.waitForTopic(Topic.GROUP_METADATA_TOPIC_NAME, 1);
        }
    }

    public static class GroupPrincipalBuilder extends DefaultKafkaPrincipalBuilder {
        public GroupPrincipalBuilder() {
            super(null, null);
        }

        @Override
        public KafkaPrincipal build(AuthenticationContext context) {
            String listenerName = context.listenerName();
            KafkaPrincipal principal;
            switch (listenerName) {
                case BROKER_LISTENER_NAME:
                    principal = BROKER_PRINCIPAL;
                    break;
                case CONTROLLER_LISTENER_NAME:
                    principal = BROKER_PRINCIPAL;
                    break;
                case CLIENT_LISTENER_NAME:
                    principal = CLIENT_PRINCIPAL;
                    break;
                default:
                    System.out.println("No principal mapped to listener: " + listenerName);
                    throw new IllegalArgumentException("No principal mapped to listener " + listenerName);
            }
            return principal;
        }
    }

    private AccessControlEntry createAcl(AclOperation aclOperation,
                                         AclPermissionType aclPermissionType,
                                         KafkaPrincipal principal) {
        return new AccessControlEntry(
                principal.toString(),
                WILDCARD_HOST,
                aclOperation,
                aclPermissionType
        );
    }

    private void addAndVerifyAcls(Set<AccessControlEntry> acls, ResourcePattern resource, ClusterInstance clusterInstance) throws InterruptedException {
        List<AclBinding> aclBindings = acls.stream().map(acl -> new AclBinding(resource, acl)).toList();
        Authorizer authorizer = getAuthorizer(clusterInstance);
        authorizer.createAcls(ANONYMOUS_CONTEXT, aclBindings)
                .forEach(future -> {
                    try {
                        future.toCompletableFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException("Failed to create ACLs", e);
                    }
                });
        AclBindingFilter aclBindingFilter = new AclBindingFilter(resource.toFilter(), AccessControlEntryFilter.ANY);
        clusterInstance.waitAcls(aclBindingFilter, acls);

    }
    
    private void removeAndVerifyAcls(Set<AccessControlEntry> deleteAcls, ResourcePattern resource, Authorizer authorizer) throws InterruptedException {
        List<AclBindingFilter> collect = deleteAcls.stream().map(acl -> new AclBindingFilter(resource.toFilter(), acl.toFilter())).collect(Collectors.toList());
        authorizer.deleteAcls(ANONYMOUS_CONTEXT, collect)
                .forEach(future -> {
                    try {
                        future.toCompletableFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException("Failed to delete ACLs", e);
                    }
                });

        AclBindingFilter aclBindingFilter = new AclBindingFilter(resource.toFilter(), AccessControlEntryFilter.ANY);

        TestUtils.waitForCondition(() -> {
            Iterable<AclBinding> acls = authorizer.acls(aclBindingFilter);
            Set<AclBinding> aclSet = new HashSet<>();
            acls.forEach(aclSet::add);
            return aclSet.isEmpty();
        }, "Failed to verify ACLs delete");
    }

    static final AuthorizableRequestContext ANONYMOUS_CONTEXT = new AuthorizableRequestContext() {
        @Override
        public String listenerName() {
            return "";
        }

        @Override
        public SecurityProtocol securityProtocol() {
            return SecurityProtocol.PLAINTEXT;
        }

        @Override
        public KafkaPrincipal principal() {
            return KafkaPrincipal.ANONYMOUS;
        }

        @Override
        public InetAddress clientAddress() {
            return null;
        }

        @Override
        public int requestType() {
            return 0;
        }

        @Override
        public int requestVersion() {
            return 0;
        }

        @Override
        public String clientId() {
            return "";
        }

        @Override
        public int correlationId() {
            return 0;
        }
    };

    @ClusterTest(
        brokerListener = "CLIENT"
    )
    public void testUnauthorizedProduceAndConsumeWithClassicConsumer(ClusterInstance clusterInstance) throws InterruptedException {
        testUnauthorizedProduceAndConsume(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(
            brokerListener = "CLIENT"
    )
    public void testUnauthorizedProduceAndConsumeWithAsyncConsumer(ClusterInstance clusterInstance) throws InterruptedException {
        testUnauthorizedProduceAndConsume(clusterInstance, GroupProtocol.CONSUMER);
    }
//todo
    public void testUnauthorizedProduceAndConsume(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException {
        setup(clusterInstance);

        String topic = "topic";
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        Admin admin = clusterInstance.admin();
        Producer<byte[], byte[]> producer = clusterInstance.producer();
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT)
        ));
        
        try {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic));

            ExecutionException produceException = assertThrows(
                ExecutionException.class,
                () -> producer.send(new ProducerRecord<>(topic, "message".getBytes())).get()
            );

            Throwable cause = produceException.getCause();
            assertInstanceOf(TopicAuthorizationException.class, cause);
            TopicAuthorizationException topicAuthException = (TopicAuthorizationException) cause;
            assertEquals(Set.of(topic), topicAuthException.unauthorizedTopics());

            consumer.assign(Collections.singletonList(topicPartition));
            TopicAuthorizationException consumeException = assertThrows(
                TopicAuthorizationException.class,
                () -> consumer.poll(Duration.ofSeconds(10))
            );

            assertEquals(consumeException.unauthorizedTopics(), topicAuthException.unauthorizedTopics());
        } finally {
            producer.close(Duration.ZERO);
            consumer.close();
            admin.close();
        }
    }


    @ClusterTest(
        brokerListener = "CLIENT"
    )
    public void testClassicConsumeUnsubscribeWithoutGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeUnsubscribeWithoutGroupPermission(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(
            brokerListener = "CLIENT"
    )
//    fail
    public void testAsyncConsumeUnsubscribeWithoutGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeUnsubscribeWithoutGroupPermission(clusterInstance, GroupProtocol.CONSUMER);
    }
//todo1
    private void testConsumeUnsubscribeWithoutGroupPermission(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        setup(clusterInstance);
        
        String topic = "topic";

        // allow topic read/write permission to poll/send record
        Set<AccessControlEntry> acls = new HashSet<>();
        acls.add(createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        acls.add(createAcl(AclOperation.WRITE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        acls.add(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        addAndVerifyAcls(
            acls,
            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
            clusterInstance
        );

        Admin admin = clusterInstance.admin();
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        admin.createTopics(Collections.singleton(newTopic));
        clusterInstance.waitForTopic(topic, 1);
        admin.close();

        Producer<byte[], byte[]> producer = clusterInstance.producer();
        // Send a message
        producer.send(new ProducerRecord<>(topic, "message".getBytes())).get();
        producer.close();

        String group = "group";
        addAndVerifyAcls(
            Collections.singleton(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
            new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
            clusterInstance
        );

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfigs.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT));
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(consumerConfigs);
        consumer.subscribe(Collections.singletonList(topic));

        TestUtils.waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(15));
            return records.count() == 1;
        }, "poll message");

//        here
        removeAndVerifyAcls(
                Collections.singleton(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                getAuthorizer(clusterInstance)
        );

        // Test unsubscribe fail
//        consumer.unsubscribe();
        assertDoesNotThrow(() -> consumer.unsubscribe());
        consumer.close();
    }

    @ClusterTest(
        brokerListener = "CLIENT"
    )
    public void testClassicConsumeCloseWithoutGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeCloseWithoutGroupPermission(clusterInstance, GroupProtocol.CLASSIC);
    }

//    slow
    @ClusterTest(
            brokerListener = "CLIENT"
    )
    public void testAsyncConsumeCloseWithoutGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeCloseWithoutGroupPermission(clusterInstance, GroupProtocol.CONSUMER);
    }

//    todo2
    private void testConsumeCloseWithoutGroupPermission(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        String topic = "topic-1";

        // allow topic read/write permission to poll/send record
        Set<AccessControlEntry> acls = new HashSet<>();
        acls.add(createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        acls.add(createAcl(AclOperation.WRITE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        acls.add(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));

        addAndVerifyAcls(
                acls,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                clusterInstance
        );

        Admin admin = clusterInstance.admin();
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        admin.createTopics(Collections.singleton(newTopic));

        Producer<Object, Object> producer = clusterInstance.producer();
        producer.send(new ProducerRecord<>(topic, "message".getBytes())).get();
        producer.close();

        // allow group read permission to join group
        String group = "group";
        addAndVerifyAcls(
                Collections.singleton(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                clusterInstance
        );

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfigs.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT));
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(consumerConfigs);
        consumer.subscribe(Collections.singletonList(topic));
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
            return records.count() == 1;
        }, "poll message");

//        here
        removeAndVerifyAcls(
                Collections.singleton(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                getAuthorizer(clusterInstance)
        );

        assertDoesNotThrow(() -> consumer.close());
        admin.close();
    }

    @ClusterTest(
        brokerListener = "CLIENT"
    )
    public void testAuthorizedProduceAndConsumeWithClassic(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testAuthorizedProduceAndConsume(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(
            brokerListener = "CLIENT"
    )
    public void testAuthorizedProduceAndConsumeWithAsync(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testAuthorizedProduceAndConsume(clusterInstance, GroupProtocol.CONSUMER);
    }

//    todo3
    private void testAuthorizedProduceAndConsume(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        setup(clusterInstance);
        String topic = "topic";
        TopicPartition topicPartition = new TopicPartition("topic", 0);

        AccessControlEntry acl = createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL);
        AccessControlEntry acl1 = createAcl(AclOperation.WRITE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL);
        AccessControlEntry acl2 = createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL);
        Set<AccessControlEntry> aclList = new HashSet<>();
        aclList.add(acl);
        aclList.add(acl1);
        aclList.add(acl2);

        addAndVerifyAcls(
                aclList,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                clusterInstance
        );

        Admin admin = clusterInstance.admin();
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        admin.createTopics(Collections.singleton(newTopic));
        clusterInstance.waitForTopic(topic, 1);

        Producer<Object, Object> producer = clusterInstance.producer();
        producer.send(new ProducerRecord<>(topic, "message".getBytes())).get();
        producer.close();

        String group = "group";
        addAndVerifyAcls(
                Collections.singleton(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                clusterInstance
        );

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfigs.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT));
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(consumerConfigs);
        consumer.assign(List.of(topicPartition));
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            return records.count() == 1;
        }, "Failed to receive message");
        admin.close();
        consumer.close();

    }

}
