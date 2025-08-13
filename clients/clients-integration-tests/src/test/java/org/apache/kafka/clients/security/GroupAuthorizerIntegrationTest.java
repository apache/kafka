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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;


@ClusterTestDefaults(serverProperties = {
    @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "Group:broker"),
    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
    @ClusterConfigProperty(key = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, value = "org.apache.kafka.clients.security.GroupAuthorizerIntegrationTest$GroupPrincipalBuilder"),
})
public class GroupAuthorizerIntegrationTest {
    private static final KafkaPrincipal BROKER_PRINCIPAL = new KafkaPrincipal("Group", "broker");
    private static final KafkaPrincipal CLIENT_PRINCIPAL = new KafkaPrincipal("Group", "client");

    private static final String BROKER_LISTENER_NAME = "BROKER";
    private static final String CLIENT_LISTENER_NAME = "EXTERNAL";
    private static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";

    private Authorizer getAuthorizer(ClusterInstance clusterInstance) {
        return clusterInstance.controllers().values().stream()
                .filter(server -> server.authorizerPlugin().isDefined())
                .map(server -> server.authorizerPlugin().get().get()).findFirst().get();
    }

    private void setup(ClusterInstance clusterInstance) throws InterruptedException {
        // Allow inter-broker communication
        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.CLUSTER_ACTION, AclPermissionType.ALLOW, BROKER_PRINCIPAL)),
                new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
                clusterInstance
        );
        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME, PatternType.LITERAL),
                clusterInstance
        );

        NewTopic offsetTopic = new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 1, (short) 1);
        try (Admin admin = clusterInstance.admin(Map.of(
                AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, true))
        ) {
            admin.createTopics(Collections.singleton(offsetTopic));
            clusterInstance.waitTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, 1);
        }
    }

    public static class GroupPrincipalBuilder extends DefaultKafkaPrincipalBuilder {
        public GroupPrincipalBuilder() {
            super(null, null);
        }

        @Override
        public KafkaPrincipal build(AuthenticationContext context) {
            String listenerName = context.listenerName();
            return switch (listenerName) {
                case BROKER_LISTENER_NAME, CONTROLLER_LISTENER_NAME -> BROKER_PRINCIPAL;
                case CLIENT_LISTENER_NAME -> CLIENT_PRINCIPAL;
                default -> throw new IllegalArgumentException("No principal mapped to listener " + listenerName);
            };
        }
    }

    private AccessControlEntry createAcl(AclOperation aclOperation, AclPermissionType aclPermissionType, KafkaPrincipal principal) {
        return new AccessControlEntry(
                principal.toString(),
                WILDCARD_HOST,
                aclOperation,
                aclPermissionType
        );
    }

    private void addAndVerifyAcls(Set<AccessControlEntry> acls, ResourcePattern resource, ClusterInstance clusterInstance) throws InterruptedException {
        List<AclBinding> aclBindings = acls.stream()
                .map(acl -> new AclBinding(resource, acl))
                .toList();
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

    private void removeAndVerifyAcls(Set<AccessControlEntry> deleteAcls, ResourcePattern resource, ClusterInstance clusterInstance) throws InterruptedException {
        List<AclBindingFilter> aclBindingFilters = deleteAcls.stream()
                .map(acl -> new AclBindingFilter(resource.toFilter(), acl.toFilter()))
                .toList();
        Authorizer authorizer = getAuthorizer(clusterInstance);
        authorizer.deleteAcls(ANONYMOUS_CONTEXT, aclBindingFilters)
                .forEach(future -> {
                    try {
                        future.toCompletableFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException("Failed to delete ACLs", e);
                    }
                });

        AclBindingFilter aclBindingFilter = new AclBindingFilter(resource.toFilter(), AccessControlEntryFilter.ANY);
        TestUtils.waitForCondition(() -> {
            Set<AccessControlEntry> remainingAclEntries = new HashSet<>();
            authorizer.acls(aclBindingFilter).forEach(aclBinding -> remainingAclEntries.add(aclBinding.entry()));
            return deleteAcls.stream().noneMatch(remainingAclEntries::contains);
        }, "Failed to verify ACLs deletion");
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

    @ClusterTest
    public void testUnauthorizedProduceAndConsumeWithClassicConsumer(ClusterInstance clusterInstance) throws InterruptedException {
        testUnauthorizedProduceAndConsume(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testUnauthorizedProduceAndConsumeWithAsyncConsumer(ClusterInstance clusterInstance) throws InterruptedException {
        testUnauthorizedProduceAndConsume(clusterInstance, GroupProtocol.CONSUMER);
    }

    public void testUnauthorizedProduceAndConsume(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException {
        setup(clusterInstance);
        String topic = "topic";
        String group = "group";

        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                clusterInstance
        );
        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                clusterInstance
        );

        Producer<byte[], byte[]> producer = clusterInstance.producer();
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
                GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT),
                ConsumerConfig.GROUP_ID_CONFIG, group
        ));

        try {
            clusterInstance.createTopic(topic, 1, (short) 1);
            ExecutionException produceException = assertThrows(
                ExecutionException.class,
                () -> producer.send(new ProducerRecord<>(topic, "message".getBytes())).get()
            );
            Throwable cause = produceException.getCause();
            assertInstanceOf(TopicAuthorizationException.class, cause);
            TopicAuthorizationException topicAuthException = (TopicAuthorizationException) cause;
            assertEquals(Set.of(topic), topicAuthException.unauthorizedTopics());

            TopicPartition topicPartition = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(topicPartition));
            TopicAuthorizationException consumeException = assertThrows(
                TopicAuthorizationException.class,
                () -> consumer.poll(Duration.ofSeconds(15))
            );
            assertEquals(consumeException.unauthorizedTopics(), topicAuthException.unauthorizedTopics());
        } finally {
            producer.close(Duration.ZERO);
            consumer.close();
        }
    }

    @ClusterTest
    public void testClassicConsumeUnsubscribeWithGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeUnsubscribeWithOrWithoutGroupPermission(clusterInstance, GroupProtocol.CLASSIC, true);
    }

    @ClusterTest
    public void testAsyncConsumeUnsubscribeWithGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeUnsubscribeWithOrWithoutGroupPermission(clusterInstance, GroupProtocol.CONSUMER, true);
    }

    @ClusterTest
    public void testClassicConsumeUnsubscribeWithoutGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeUnsubscribeWithOrWithoutGroupPermission(clusterInstance, GroupProtocol.CLASSIC, false);
    }

    @ClusterTest
    public void testAsyncConsumeUnsubscribeWithoutGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeUnsubscribeWithOrWithoutGroupPermission(clusterInstance, GroupProtocol.CONSUMER, false);
    }

    private void testConsumeUnsubscribeWithOrWithoutGroupPermission(ClusterInstance clusterInstance, GroupProtocol groupProtocol, boolean withGroupPermission) throws InterruptedException, ExecutionException {
        setup(clusterInstance);
        String topic = "topic";
        String group = "group";

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
        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                clusterInstance
        );

        try (Producer<byte[], byte[]> producer = clusterInstance.producer();
            Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT)))
        ) {
            clusterInstance.createTopic(topic, 1, (short) 1);
            producer.send(new ProducerRecord<>(topic, "message".getBytes())).get();
            consumer.subscribe(Collections.singletonList(topic));
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(15));
                return records.count() == 1;
            }, "consumer failed to receive message");
            if (!withGroupPermission) {
                removeAndVerifyAcls(
                        Set.of(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                        new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                        clusterInstance
                );
            }
            assertDoesNotThrow(consumer::unsubscribe);
        }
    }

    @ClusterTest
    public void testClassicConsumeCloseWithGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeCloseWithGroupPermission(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumeCloseWithGroupPermission(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testConsumeCloseWithGroupPermission(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void testConsumeCloseWithGroupPermission(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        setup(clusterInstance);
        String topic = "topic";
        String group = "group";

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
        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                clusterInstance
        );

        Producer<Object, Object> producer = clusterInstance.producer();
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT)));

        try {
            clusterInstance.createTopic(topic, 1, (short) 1);
            producer.send(new ProducerRecord<>(topic, "message".getBytes())).get();
            consumer.subscribe(List.of(topic));
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(15));
                return records.count() == 1;
            }, "consumer failed to receive message");
        } finally {
            producer.close();
            assertDoesNotThrow(() -> consumer.close());
        }
    }

    @ClusterTest
    public void testAuthorizedProduceAndConsumeWithClassic(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testAuthorizedProduceAndConsume(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAuthorizedProduceAndConsumeWithAsync(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        testAuthorizedProduceAndConsume(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void testAuthorizedProduceAndConsume(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        setup(clusterInstance);
        String topic = "topic";
        String group = "group";

        Set<AccessControlEntry> acls = new HashSet<>();
        acls.add(createAcl(AclOperation.CREATE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        acls.add(createAcl(AclOperation.WRITE, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        acls.add(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL));
        addAndVerifyAcls(
                acls,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                clusterInstance
        );
        addAndVerifyAcls(
                Set.of(createAcl(AclOperation.READ, AclPermissionType.ALLOW, CLIENT_PRINCIPAL)),
                new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
                clusterInstance
        );

        try (Producer<byte[], byte[]> producer = clusterInstance.producer();
             Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
                     ConsumerConfig.GROUP_ID_CONFIG, group,
                     ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                     GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT)))
        ) {
            clusterInstance.createTopic(topic, 1, (short) 1);
            producer.send(new ProducerRecord<>(topic, "message".getBytes())).get();
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            consumer.assign(List.of(topicPartition));
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(15));
                return records.count() == 1;
            }, "consumer failed to receive message");
        }
    }

}
