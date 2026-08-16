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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:broker"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = ConsumerGroupCommandAuthorizationTest.STANDARD_AUTHORIZER),
        @ClusterConfigProperty(key = PRINCIPAL_BUILDER_CLASS_CONFIG, value = ConsumerGroupCommandAuthorizationTest.PRINCIPAL_BUILDER)
    }
)
public class ConsumerGroupCommandAuthorizationTest {
    public static final String STANDARD_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";
    public static final String PRINCIPAL_BUILDER = "org.apache.kafka.tools.consumer.group.ConsumerGroupCommandAuthorizationTest$PrincipalBuilder";

    private static final KafkaPrincipal BROKER_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "broker");
    private static final KafkaPrincipal CLIENT_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client");

    private static final String CLIENT_LISTENER_NAME = "EXTERNAL";
    private static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";

    private static final String GROUP = "my-group";
    private static final ResourcePattern GROUP_RESOURCE = new ResourcePattern(ResourceType.GROUP, GROUP, PatternType.LITERAL);
    private static final ResourcePattern OFFSETS_TOPIC_RESOURCE = new ResourcePattern(
        ResourceType.TOPIC,
        Topic.GROUP_METADATA_TOPIC_NAME,
        PatternType.LITERAL
    );

    @ClusterTest
    public void testDescribeGroupCliWithGroupDescribe(ClusterInstance clusterInstance) throws Exception {
        addAndVerifyAcls(Set.of(createAcl(CREATE, CLIENT_PRINCIPAL)), OFFSETS_TOPIC_RESOURCE, clusterInstance);

        NewTopic offsetTopic = new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 1, (short) 1);
        try (Admin admin = clusterInstance.admin()) {
            admin.createTopics(List.of(offsetTopic)).all().get();
            clusterInstance.waitTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, 1);
        }

        addAndVerifyAcls(Set.of(createAcl(DESCRIBE, CLIENT_PRINCIPAL)), GROUP_RESOURCE, clusterInstance);

        String[] cgcArgs = new String[]{
            "--bootstrap-server", clusterInstance.bootstrapServers(),
            "--describe",
            "--group", GROUP
        };
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(cgcArgs);
        try (ConsumerGroupCommand.ConsumerGroupService consumerGroupService = new ConsumerGroupCommand.ConsumerGroupService(opts, Map.of())) {
            ExecutionException e = assertThrows(ExecutionException.class, consumerGroupService::describeGroups);
            assertInstanceOf(GroupIdNotFoundException.class, e.getCause(),
                "Non-existent group should throw GroupIdNotFoundException");
        }
    }

    private static AccessControlEntry createAcl(AclOperation operation, KafkaPrincipal principal) {
        return new AccessControlEntry(principal.toString(), WILDCARD_HOST, operation, ALLOW);
    }

    private void addAndVerifyAcls(Set<AccessControlEntry> acls, ResourcePattern resource, ClusterInstance clusterInstance) throws Exception {
        List<AclBinding> aclBindings = acls.stream()
            .map(acl -> new AclBinding(resource, acl))
            .toList();

        try (Admin admin = clusterInstance.admin(Map.of(), true)) {
            admin.createAcls(aclBindings).all().get();
        }

        clusterInstance.waitAcls(new AclBindingFilter(resource.toFilter(), AccessControlEntryFilter.ANY), acls);
    }

    public static class PrincipalBuilder extends DefaultKafkaPrincipalBuilder {
        public PrincipalBuilder() {
            super(null, null);
        }

        @Override
        public KafkaPrincipal build(AuthenticationContext context) {
            return switch (context.listenerName()) {
                case CLIENT_LISTENER_NAME -> CLIENT_PRINCIPAL;
                case CONTROLLER_LISTENER_NAME -> BROKER_PRINCIPAL;
                default -> throw new IllegalArgumentException("No principal mapped to listener " + context.listenerName());
            };
        }
    }
}
