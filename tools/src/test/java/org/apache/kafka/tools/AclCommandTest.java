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
package org.apache.kafka.tools;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.test.TestUtils;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;

import static org.apache.kafka.common.acl.AccessControlEntryFilter.ANY;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.CREATE_TOKENS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_TOKENS;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.DELEGATION_TOKEN;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.common.resource.ResourceType.USER;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
            @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = AclCommandTest.STANDARD_AUTHORIZER)}

)
@ExtendWith(ClusterTestExtensions.class)
public class AclCommandTest {
    public static final String STANDARD_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";
    private static final String LOCALHOST = "localhost:9092";
    private static final String ADD = "--add";
    private static final String BOOTSTRAP_SERVER = "--bootstrap-server";
    private static final String BOOTSTRAP_CONTROLLER = "--bootstrap-controller";
    private static final String COMMAND_CONFIG = "--command-config";
    private static final String CONSUMER = "--consumer";
    private static final String IDEMPOTENT = "--idempotent";
    private static final String GROUP = "--group";
    private static final String LIST = "--list";
    private static final String REMOVE = "--remove";
    private static final String PRODUCER = "--producer";
    private static final String OPERATION = "--operation";
    private static final String TOPIC = "--topic";
    private static final String RESOURCE_PATTERN_TYPE = "--resource-pattern-type";
    private static final KafkaPrincipal PRINCIPAL = SecurityUtils.parseKafkaPrincipal("User:test2");
    private static final Set<KafkaPrincipal> USERS = Set.of(
        SecurityUtils.parseKafkaPrincipal("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
        PRINCIPAL,
        SecurityUtils.parseKafkaPrincipal("User:CN=\\#User with special chars in CN : (\\, \\+ \" \\ \\< \\> \\; ')")
    );
    private static final Set<String> HOSTS = Set.of("host1", "host2");
    private static final List<String> ALLOW_HOST_COMMAND = List.of("--allow-host", "host1", "--allow-host", "host2");
    private static final List<String> DENY_HOST_COMMAND = List.of("--deny-host", "host1", "--deny-host", "host2");

    private static final ResourcePattern CLUSTER_RESOURCE = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL);
    private static final Set<ResourcePattern> TOPIC_RESOURCES = Set.of(
        new ResourcePattern(ResourceType.TOPIC, "test-1", LITERAL),
        new ResourcePattern(ResourceType.TOPIC, "test-2", LITERAL)
    );
    private static final Set<ResourcePattern> GROUP_RESOURCES = Set.of(
        new ResourcePattern(ResourceType.GROUP, "testGroup-1", LITERAL),
        new ResourcePattern(ResourceType.GROUP, "testGroup-2", LITERAL)
    );
    private static final Set<ResourcePattern> TRANSACTIONAL_ID_RESOURCES = Set.of(
            new ResourcePattern(TRANSACTIONAL_ID, "t0", LITERAL),
            new ResourcePattern(TRANSACTIONAL_ID, "t1", LITERAL)
    );
    private static final Set<ResourcePattern> TOKEN_RESOURCES = Set.of(
        new ResourcePattern(DELEGATION_TOKEN, "token1", LITERAL),
        new ResourcePattern(DELEGATION_TOKEN, "token2", LITERAL)
    );
    private static final Set<ResourcePattern> USER_RESOURCES = Set.of(
        new ResourcePattern(USER, "User:test-user1", LITERAL),
        new ResourcePattern(USER, "User:test-user2", LITERAL)
    );

    private static final Map<Set<ResourcePattern>, List<String>> RESOURCE_TO_COMMAND = Map.of(
        TOPIC_RESOURCES, List.of(TOPIC, "test-1", TOPIC, "test-2"),
        Set.of(CLUSTER_RESOURCE), List.of("--cluster"),
        GROUP_RESOURCES, List.of(GROUP, "testGroup-1", GROUP, "testGroup-2"),
        TRANSACTIONAL_ID_RESOURCES, List.of("--transactional-id", "t0", "--transactional-id", "t1"),
        TOKEN_RESOURCES, List.of("--delegation-token", "token1", "--delegation-token", "token2"),
        USER_RESOURCES, List.of("--user-principal", "User:test-user1", "--user-principal", "User:test-user2")
    );

    private static final Map<Set<ResourcePattern>, Map.Entry<Set<AclOperation>, List<String>>> RESOURCE_TO_OPERATIONS = Map.of(
        TOPIC_RESOURCES, Map.entry(
            Set.of(READ, WRITE, CREATE, DESCRIBE, DELETE, DESCRIBE_CONFIGS, ALTER_CONFIGS, ALTER),
            List.of(OPERATION, "Read", OPERATION, "Write", OPERATION, "Create",
                    OPERATION, "Describe", OPERATION, "Delete", OPERATION, "DescribeConfigs",
                    OPERATION, "AlterConfigs", OPERATION, "Alter")),
        Set.of(CLUSTER_RESOURCE), Map.entry(
            Set.of(CREATE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALTER, DESCRIBE),
            List.of(OPERATION, "Create", OPERATION, "ClusterAction", OPERATION, "DescribeConfigs",
                    OPERATION, "AlterConfigs", OPERATION, "IdempotentWrite", OPERATION, "Alter", OPERATION, "Describe")),
        GROUP_RESOURCES, Map.entry(
            Set.of(READ, DESCRIBE, DELETE),
            List.of(OPERATION, "Read", OPERATION, "Describe", OPERATION, "Delete")),
        TRANSACTIONAL_ID_RESOURCES, Map.entry(
            Set.of(DESCRIBE, WRITE),
            List.of(OPERATION, "Describe", OPERATION, "Write")),
        TOKEN_RESOURCES, Map.entry(
            Set.of(DESCRIBE),
            List.of(OPERATION, "Describe")),
        USER_RESOURCES, Map.entry(
            Set.of(CREATE_TOKENS, DESCRIBE_TOKENS),
            List.of(OPERATION, "CreateTokens", OPERATION, "DescribeTokens"))
    );

    private static final Map<Set<ResourcePattern>, Set<AccessControlEntry>> CONSUMER_RESOURCE_TO_ACLS = Map.of(
        TOPIC_RESOURCES, AclCommand.getAcls(USERS, ALLOW, Set.of(READ, DESCRIBE), HOSTS),
        GROUP_RESOURCES, AclCommand.getAcls(USERS, ALLOW, Set.of(READ), HOSTS)
    );

    private static final Map<List<String>, Map<Set<ResourcePattern>, Set<AccessControlEntry>>> CMD_TO_RESOURCES_TO_ACL = Map.of(
        List.of(PRODUCER), producerResourceToAcls(false),
        List.of(PRODUCER, IDEMPOTENT), producerResourceToAcls(true),
        List.of(CONSUMER), CONSUMER_RESOURCE_TO_ACLS,
        List.of(PRODUCER, CONSUMER),
            CONSUMER_RESOURCE_TO_ACLS.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    Set<AccessControlEntry> value = new HashSet<>(entry.getValue());
                    value.addAll(producerResourceToAcls(false).getOrDefault(entry.getKey(), Set.of()));
                    return value;
                }
            )),
        List.of(PRODUCER, IDEMPOTENT, CONSUMER),
            CONSUMER_RESOURCE_TO_ACLS.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    Set<AccessControlEntry> value = new HashSet<>(entry.getValue());
                    value.addAll(producerResourceToAcls(true).getOrDefault(entry.getKey(), Set.of()));
                    return value;
                }
            ))
    );

    @ClusterTest
    public void testAclCliWithAdminAPI(ClusterInstance cluster) throws InterruptedException {
        testAclCli(cluster, adminArgs(cluster.bootstrapServers(), Optional.empty()));
    }


    @ClusterTest
    public void testAclCliWithAdminAPIAndBootstrapController(ClusterInstance cluster) throws InterruptedException {
        testAclCli(cluster, adminArgsWithBootstrapController(cluster.bootstrapControllers(), Optional.empty()));
    }

    @ClusterTest
    public void testAclCliWithMisusingBootstrapServerToController(ClusterInstance cluster) {
        assertThrows(RuntimeException.class, () -> testAclCli(cluster, adminArgsWithBootstrapController(cluster.bootstrapServers(), Optional.empty())));
    }

    @ClusterTest
    public void testAclCliWithMisusingBootstrapControllerToServer(ClusterInstance cluster) {
        assertThrows(RuntimeException.class, () -> testAclCli(cluster, adminArgs(cluster.bootstrapControllers(), Optional.empty())));
    }

    @ClusterTest
    public void testProducerConsumerCliWithAdminAPI(ClusterInstance cluster) throws InterruptedException {
        testProducerConsumerCli(cluster, adminArgs(cluster.bootstrapServers(), Optional.empty()));
    }

    @ClusterTest
    public void testProducerConsumerCliWithAdminAPIAndBootstrapController(ClusterInstance cluster) throws InterruptedException {
        testProducerConsumerCli(cluster, adminArgsWithBootstrapController(cluster.bootstrapControllers(), Optional.empty()));
    }

    @ClusterTest
    public void testAclCliWithClientId(ClusterInstance cluster) throws IOException, InterruptedException {
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(AppInfoParser.class, Level.WARN);
            testAclCli(cluster, adminArgs(cluster.bootstrapServers(), Optional.of(TestUtils.tempFile("client.id=my-client"))));
            assertEquals(0, appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals(Level.WARN.toString()))
                    .filter(e -> e.getThrowableClassName().filter(name -> name.equals(InstanceAlreadyExistsException.class.getName())).isPresent())
                    .count(), "There should be no warnings about multiple registration of mbeans");
        }
    }

    @ClusterTest
    public void testAclCliWithClientIdAndBootstrapController(ClusterInstance cluster) throws IOException, InterruptedException {
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(AppInfoParser.class, Level.WARN);
            testAclCli(cluster, adminArgsWithBootstrapController(cluster.bootstrapControllers(), Optional.of(TestUtils.tempFile("client.id=my-client"))));
            assertEquals(0, appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals(Level.WARN.toString()))
                    .filter(e -> e.getThrowableClassName().filter(name -> name.equals(InstanceAlreadyExistsException.class.getName())).isPresent())
                    .count(), "There should be no warnings about multiple registration of mbeans");
        }
    }

    @ClusterTest
    public void testAclsOnPrefixedResourcesWithAdminAPI(ClusterInstance cluster) throws InterruptedException {
        testAclsOnPrefixedResources(cluster, adminArgs(cluster.bootstrapServers(), Optional.empty()));
    }

    @ClusterTest
    public void testAclsOnPrefixedResourcesWithAdminAPIAndBootstrapController(ClusterInstance cluster) throws InterruptedException {
        testAclsOnPrefixedResources(cluster, adminArgsWithBootstrapController(cluster.bootstrapControllers(), Optional.empty()));
    }

    @ClusterTest
    public void testPatternTypesWithAdminAPI(ClusterInstance cluster) {
        testPatternTypes(adminArgs(cluster.bootstrapServers(), Optional.empty()));
    }

    @ClusterTest
    public void testPatternTypesWithAdminAPIAndBootstrapController(ClusterInstance cluster) {
        testPatternTypes(adminArgsWithBootstrapController(cluster.bootstrapControllers(), Optional.empty()));
    }

    @Test
    public void testUseBootstrapServerOptWithBootstrapControllerOpt() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, BOOTSTRAP_CONTROLLER, LOCALHOST),
                "Only one of --bootstrap-server or --bootstrap-controller must be specified"
        );
    }

    @Test
    public void testUseWithoutBootstrapServerOptAndBootstrapControllerOpt() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(ADD),
                "One of --bootstrap-server or --bootstrap-controller must be specified"
        );
    }

    @Test
    public void testExactlyOneAction() {
        String errMsg = "Command must include exactly one action: --list, --add, --remove. ";
        assertInitializeInvalidOptionsExitCodeAndMsg(List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, LIST), errMsg);
        assertInitializeInvalidOptionsExitCodeAndMsg(List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, LIST, REMOVE), errMsg);
    }

    @Test
    public void testUseListPrincipalsOptWithoutListOpt() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, "--principal", "User:CN=client"),
                "The --principal option is only available if --list is set"
        );
    }

    @Test
    public void testUseProducerOptWithoutTopicOpt() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, PRODUCER),
                "With --producer you must specify a --topic"
        );
    }

    @Test
    public void testUseIdempotentOptWithoutProducerOpt() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, IDEMPOTENT),
                "The --idempotent option is only available if --producer is set"
        );
    }

    @Test
    public void testUseConsumerOptWithoutRequiredOpt() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, CONSUMER),
                "With --consumer you must specify a --topic and a --group and no --cluster or --transactional-id option should be specified."
        );
        checkNotThrow(List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, CONSUMER, TOPIC, "test-topic", GROUP, "test-group"));
    }

    @Test
    public void testInvalidArgs() {
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, LIST, PRODUCER),
                "Option \"[list]\" can't be used with option \"[producer]\""
        );
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, PRODUCER, OPERATION, "all"),
                "Option \"[producer]\" can't be used with option \"[operation]\""
        );
        assertInitializeInvalidOptionsExitCodeAndMsg(
                List.of(BOOTSTRAP_SERVER, LOCALHOST, ADD, CONSUMER, OPERATION, TOPIC, "test-topic", GROUP, "test-group"),
                "Option \"[consumer]\" can't be used with option \"[operation]\""
        );
    }

    private void testProducerConsumerCli(ClusterInstance cluster, List<String> cmdArgs) throws InterruptedException {
        for (Map.Entry<List<String>, Map<Set<ResourcePattern>, Set<AccessControlEntry>>> entry : CMD_TO_RESOURCES_TO_ACL.entrySet()) {
            List<String> cmd = entry.getKey();
            Map<Set<ResourcePattern>, Set<AccessControlEntry>> resourcesToAcls = entry.getValue();
            List<String> resourceCommand = resourcesToAcls.keySet().stream()
                    .map(RESOURCE_TO_COMMAND::get)
                    .reduce(new ArrayList<>(), (list, commands) -> {
                        list.addAll(commands);
                        return list;
                    });

            List<String> args = new ArrayList<>(cmdArgs);
            args.addAll(getCmd(ALLOW));
            args.addAll(resourceCommand);
            args.addAll(cmd);
            args.add(ADD);
            callMain(args);

            for (Map.Entry<Set<ResourcePattern>, Set<AccessControlEntry>> resourcesToAclsEntry : resourcesToAcls.entrySet()) {
                for (ResourcePattern resource : resourcesToAclsEntry.getKey()) {
                    cluster.waitAcls(new AclBindingFilter(resource.toFilter(), ANY), resourcesToAclsEntry.getValue());
                }
            }
            List<String> resourceCmd = new ArrayList<>(resourceCommand);
            resourceCmd.addAll(cmd);
            testRemove(cluster, cmdArgs, resourcesToAcls.keySet().stream().flatMap(Set::stream).collect(Collectors.toSet()), resourceCmd);
        }
    }

    private void testAclsOnPrefixedResources(ClusterInstance cluster, List<String> cmdArgs) throws InterruptedException {
        List<String> cmd = List.of("--allow-principal", PRINCIPAL.toString(), PRODUCER, TOPIC, "Test-", RESOURCE_PATTERN_TYPE, "Prefixed");

        List<String> args = new ArrayList<>(cmdArgs);
        args.addAll(cmd);
        args.add(ADD);
        callMain(args);

        AccessControlEntry writeAcl = new AccessControlEntry(PRINCIPAL.toString(), WILDCARD_HOST, WRITE, ALLOW);
        AccessControlEntry describeAcl = new AccessControlEntry(PRINCIPAL.toString(), WILDCARD_HOST, DESCRIBE, ALLOW);
        AccessControlEntry createAcl = new AccessControlEntry(PRINCIPAL.toString(), WILDCARD_HOST, CREATE, ALLOW);
        cluster.waitAcls(new AclBindingFilter(new ResourcePattern(ResourceType.TOPIC, "Test-", PREFIXED).toFilter(), ANY),
                List.of(writeAcl, describeAcl, createAcl));

        args = new ArrayList<>(cmdArgs);
        args.addAll(cmd);
        args.add(REMOVE);
        args.add("--force");
        callMain(args);

        cluster.waitAcls(new AclBindingFilter(new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PREFIXED).toFilter(), ANY),
                Set.of());
        cluster.waitAcls(new AclBindingFilter(new ResourcePattern(ResourceType.TOPIC, "Test-", PREFIXED).toFilter(), ANY),
                Set.of());
    }

    private static Map<Set<ResourcePattern>, Set<AccessControlEntry>> producerResourceToAcls(boolean enableIdempotence) {
        return Map.of(
            TOPIC_RESOURCES, AclCommand.getAcls(USERS, ALLOW, Set.of(WRITE, DESCRIBE, CREATE), HOSTS),
            TRANSACTIONAL_ID_RESOURCES, AclCommand.getAcls(USERS, ALLOW, Set.of(WRITE, DESCRIBE), HOSTS),
            Set.of(CLUSTER_RESOURCE), AclCommand.getAcls(USERS, ALLOW,
                enableIdempotence
                        ? Set.of(IDEMPOTENT_WRITE)
                        : Set.of(), HOSTS));
    }

    private List<String> adminArgs(String bootstrapServer, Optional<File> commandConfig) {
        List<String> adminArgs = new ArrayList<>(List.of(BOOTSTRAP_SERVER, bootstrapServer));
        commandConfig.ifPresent(file -> adminArgs.addAll(List.of(COMMAND_CONFIG, file.getAbsolutePath())));
        return adminArgs;
    }

    private List<String> adminArgsWithBootstrapController(String bootstrapController, Optional<File> commandConfig) {
        List<String> adminArgs = new ArrayList<>(List.of(BOOTSTRAP_CONTROLLER, bootstrapController));
        commandConfig.ifPresent(file -> adminArgs.addAll(List.of(COMMAND_CONFIG, file.getAbsolutePath())));
        return adminArgs;
    }

    private Map.Entry<String, String> callMain(List<String> args) {
        return ToolsTestUtils.grabConsoleOutputAndError(() -> AclCommand.main(args.toArray(new String[0])));
    }

    private void testAclCli(ClusterInstance cluster, List<String> cmdArgs) throws InterruptedException {
        for (Map.Entry<Set<ResourcePattern>, List<String>> entry : RESOURCE_TO_COMMAND.entrySet()) {
            Set<ResourcePattern> resources = entry.getKey();
            List<String> resourceCmd = entry.getValue();
            Set<AclPermissionType> permissionTypes = Set.of(ALLOW, DENY);
            for (AclPermissionType permissionType : permissionTypes) {
                Map.Entry<Set<AclOperation>, List<String>> operationToCmd = RESOURCE_TO_OPERATIONS.get(resources);
                Map.Entry<Set<AccessControlEntry>, List<String>> aclToCommand = getAclToCommand(permissionType, operationToCmd.getKey());

                List<String> resultArgs = new ArrayList<>(cmdArgs);
                resultArgs.addAll(aclToCommand.getValue());
                resultArgs.addAll(resourceCmd);
                resultArgs.addAll(operationToCmd.getValue());
                resultArgs.add(ADD);

                Map.Entry<String, String> out = callMain(resultArgs);
                assertOutputContains("Adding ACLs", resources, resourceCmd, out.getKey());
                assertEquals("", out.getValue());

                for (ResourcePattern resource : resources) {
                    cluster.waitAcls(new AclBindingFilter(resource.toFilter(), ANY), aclToCommand.getKey());
                }

                resultArgs = new ArrayList<>(cmdArgs);
                resultArgs.add(LIST);

                out = callMain(resultArgs);
                assertOutputContains("Current ACLs", resources, resourceCmd, out.getKey());
                assertEquals("", out.getValue());

                testRemove(cluster, cmdArgs, resources, resourceCmd);
            }
        }
    }

    private void assertOutputContains(
            String prefix,
            Set<ResourcePattern> resources,
            List<String> resourceCmd,
            String output
    ) {
        resources.forEach(resource -> {
            String resourceType = resource.resourceType().toString();

            List<String> cmd = resource == CLUSTER_RESOURCE
                    ? List.of("kafka-cluster")
                    : resourceCmd.stream().filter(s -> !s.startsWith("--")).toList();

            cmd.forEach(name -> {
                String expected = String.format(
                        "%s for resource `ResourcePattern(resourceType=%s, name=%s, patternType=LITERAL)`:",
                        prefix, resourceType, name
                );
                assertTrue(output.contains(expected), "Substring " + expected + " not in output:\n" + output);
            });
        });
    }

    private void testPatternTypes(List<String> cmdArgs) {
        Exit.setExitProcedure((status, message) -> {
            if (status == 1)
                throw new RuntimeException("Exiting command");
            else
                throw new AssertionError("Unexpected exit with status " + status);
        });
        try {
            for (PatternType patternType : PatternType.values()) {
                List<String> addCmd = new ArrayList<>(cmdArgs);
                addCmd.addAll(List.of("--allow-principal", PRINCIPAL.toString(), PRODUCER, TOPIC, "Test",
                        ADD, RESOURCE_PATTERN_TYPE, patternType.toString()));
                verifyPatternType(addCmd, patternType.isSpecific());

                List<String> listCmd = new ArrayList<>(cmdArgs);
                listCmd.addAll(List.of(TOPIC, "Test", LIST, RESOURCE_PATTERN_TYPE, patternType.toString()));
                verifyPatternType(listCmd, patternType != PatternType.UNKNOWN);

                List<String> removeCmd = new ArrayList<>(cmdArgs);
                removeCmd.addAll(List.of(TOPIC, "Test", "--force", REMOVE, RESOURCE_PATTERN_TYPE, patternType.toString()));
                verifyPatternType(removeCmd, patternType != PatternType.UNKNOWN);
            }
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private void verifyPatternType(List<String> cmd, boolean isValid) {
        if (isValid) {
            callMain(cmd);
        } else {
            assertThrows(RuntimeException.class, () -> callMain(cmd));
        }
    }

    private void testRemove(
            ClusterInstance cluster,
            List<String> cmdArgs,
            Set<ResourcePattern> resources,
            List<String> resourceCmd
    ) throws InterruptedException {
        List<String> args = new ArrayList<>(cmdArgs);
        args.addAll(resourceCmd);
        args.add(REMOVE);
        args.add("--force");
        Map.Entry<String, String> out = callMain(args);
        assertEquals("", out.getValue());
        for (ResourcePattern resource : resources) {
            cluster.waitAcls(new AclBindingFilter(resource.toFilter(), ANY), Set.of());
        }
    }

    private Map.Entry<Set<AccessControlEntry>, List<String>> getAclToCommand(
            AclPermissionType permissionType,
            Set<AclOperation> operations
    ) {
        return Map.entry(
                AclCommand.getAcls(USERS, permissionType, operations, HOSTS),
                getCmd(permissionType)
        );
    }

    private List<String> getCmd(AclPermissionType permissionType) {
        String principalCmd = permissionType == ALLOW ? "--allow-principal" : "--deny-principal";
        List<String> cmd = permissionType == ALLOW ? ALLOW_HOST_COMMAND : DENY_HOST_COMMAND;

        List<String> fullCmd = new ArrayList<>();
        for (KafkaPrincipal user : USERS) {
            fullCmd.addAll(cmd);
            fullCmd.addAll(List.of(principalCmd, user.toString()));
        }

        return fullCmd;
    }

    private void assertInitializeInvalidOptionsExitCodeAndMsg(List<String> args, String expectedMsg) {
        Exit.setExitProcedure((exitCode, message) -> {
            assertEquals(1, exitCode);
            assertTrue(message.contains(expectedMsg));
            throw new RuntimeException();
        });
        try {
            assertThrows(RuntimeException.class, () -> new AclCommand.AclCommandOptions(args.toArray(new String[0])).checkArgs());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private void checkNotThrow(List<String> args) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });
        try {
            assertDoesNotThrow(() -> new AclCommand.AclCommandOptions(args.toArray(new String[0])).checkArgs());
            assertNull(exitStatus.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
