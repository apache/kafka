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

import kafka.admin.AclCommand.AclCommandOptions;
import kafka.security.authorizer.AclAuthorizer;
import kafka.server.KafkaConfig;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.ClusterTests;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import kafka.test.junit.RaftClusterInvocationContext;
import kafka.test.junit.ZkClusterInvocationContext;
import kafka.testkit.KafkaClusterTestKit;
import kafka.utils.Exit;
import kafka.utils.LogCaptureAppender;
import kafka.utils.TestUtils;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Authorizer;

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;

import scala.Console;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

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
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.common.resource.ResourceType.USER;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(serverProperties = {
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
        @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "kafka.security.authorizer.AclAuthorizer")
})
@ExtendWith(ClusterTestExtensions.class)
@Tag("integration")
public class AclCommandTest {
    private static final String ADD = "--add";
    private static final String STANDARD_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";
    private static final KafkaPrincipal PRINCIPAL = SecurityUtils.parseKafkaPrincipal("User:test2");
    private static final Set<KafkaPrincipal> USERS = new HashSet<>(Arrays.asList(
            SecurityUtils.parseKafkaPrincipal("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
            PRINCIPAL,
            SecurityUtils.parseKafkaPrincipal("User:CN=\\#User with special chars in CN : (\\, \\+ \" \\ \\< \\> \\; ')")
    ));
    private static final Set<String> HOSTS = new HashSet<>(Arrays.asList("host1", "host2"));
    private static final List<String> ALLOW_HOST_COMMAND = Arrays.asList("--allow-host", "host1", "--allow-host", "host2");
    private static final List<String> DENY_HOST_COMMAND = Arrays.asList("--deny-host", "host1", "--deny-host", "host2");

    private static final ResourcePattern CLUSTER_RESOURCE = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL);
    private static final Set<ResourcePattern> TOPIC_RESOURCES = new HashSet<>(Arrays.asList(
            new ResourcePattern(TOPIC, "test-1", LITERAL),
            new ResourcePattern(TOPIC, "test-2", LITERAL)
    ));
    private static final Set<ResourcePattern> GROUP_RESOURCES = new HashSet<>(Arrays.asList(
            new ResourcePattern(GROUP, "testGroup-1", LITERAL),
            new ResourcePattern(GROUP, "testGroup-2", LITERAL)
    ));
    private static final Set<ResourcePattern> TRANSACTIONAL_ID_RESOURCES = new HashSet<>(Arrays.asList(
            new ResourcePattern(TRANSACTIONAL_ID, "t0", LITERAL),
            new ResourcePattern(TRANSACTIONAL_ID, "t1", LITERAL)
    ));
    private static final Set<ResourcePattern> TOKEN_RESOURCES = new HashSet<>(Arrays.asList(
            new ResourcePattern(DELEGATION_TOKEN, "token1", LITERAL),
            new ResourcePattern(DELEGATION_TOKEN, "token2", LITERAL)
    ));
    private static final Set<ResourcePattern> USER_RESOURCES = new HashSet<>(Arrays.asList(
            new ResourcePattern(USER, "User:test-user1", LITERAL),
            new ResourcePattern(USER, "User:test-user2", LITERAL)
    ));

    private static final Map<Set<ResourcePattern>, List<String>> RESOURCE_TO_COMMAND = new HashMap<Set<ResourcePattern>, List<String>>() {{
            put(TOPIC_RESOURCES, Arrays.asList("--topic", "test-1", "--topic", "test-2"));
            put(Collections.singleton(CLUSTER_RESOURCE), Collections.singletonList("--cluster"));
            put(GROUP_RESOURCES, Arrays.asList("--group", "testGroup-1", "--group", "testGroup-2"));
            put(TRANSACTIONAL_ID_RESOURCES, Arrays.asList("--transactional-id", "t0", "--transactional-id", "t1"));
            put(TOKEN_RESOURCES, Arrays.asList("--delegation-token", "token1", "--delegation-token", "token2"));
            put(USER_RESOURCES, Arrays.asList("--user-principal", "User:test-user1", "--user-principal", "User:test-user2"));
        }};

    private static final Map<Set<ResourcePattern>, Tuple2<Set<AclOperation>, List<String>>> RESOURCE_TO_OPERATIONS =
            new HashMap<Set<ResourcePattern>, Tuple2<Set<AclOperation>, List<String>>>() {{
                put(TOPIC_RESOURCES, new Tuple2<>(
                        new HashSet<>(Arrays.asList(READ, WRITE, CREATE, DESCRIBE, DELETE, DESCRIBE_CONFIGS, ALTER_CONFIGS, ALTER)),
                        Arrays.asList("--operation", "Read", "--operation", "Write", "--operation", "Create", "--operation", "Describe", "--operation", "Delete",
                                "--operation", "DescribeConfigs", "--operation", "AlterConfigs", "--operation", "Alter"))
                );
                put(Collections.singleton(CLUSTER_RESOURCE), new Tuple2<>(
                        new HashSet<>(Arrays.asList(CREATE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALTER, DESCRIBE)),
                        Arrays.asList("--operation", "Create", "--operation", "ClusterAction", "--operation", "DescribeConfigs",
                                "--operation", "AlterConfigs", "--operation", "IdempotentWrite", "--operation", "Alter", "--operation", "Describe"))
                );
                put(GROUP_RESOURCES, new Tuple2<>(
                        new HashSet<>(Arrays.asList(READ, DESCRIBE, DELETE)),
                        Arrays.asList("--operation", "Read", "--operation", "Describe", "--operation", "Delete"))
                );
                put(TRANSACTIONAL_ID_RESOURCES, new Tuple2<>(
                        new HashSet<>(Arrays.asList(DESCRIBE, WRITE)),
                        Arrays.asList("--operation", "Describe", "--operation", "Write"))
                );
                put(TOKEN_RESOURCES, new Tuple2<>(Collections.singleton(DESCRIBE), Arrays.asList("--operation", "Describe")));
                put(USER_RESOURCES, new Tuple2<>(
                        new HashSet<>(Arrays.asList(CREATE_TOKENS, DESCRIBE_TOKENS)),
                        Arrays.asList("--operation", "CreateTokens", "--operation", "DescribeTokens"))
                );
            }};

    private static final Map<Set<ResourcePattern>, Set<AccessControlEntry>> CONSUMER_RESOURCE_TO_ACLS =
            new HashMap<Set<ResourcePattern>, Set<AccessControlEntry>>() {{
                put(TOPIC_RESOURCES, asJavaSet(AclCommand.getAcls(asScalaSet(USERS), ALLOW, asScalaSet(new HashSet<>(Arrays.asList(READ, DESCRIBE))), asScalaSet(HOSTS))));
                put(GROUP_RESOURCES, asJavaSet(AclCommand.getAcls(asScalaSet(USERS), ALLOW, asScalaSet(Collections.singleton(READ)), asScalaSet(HOSTS))));
            }};

    private static final Map<List<String>, Map<Set<ResourcePattern>, Set<AccessControlEntry>>> CMD_TO_RESOURCES_TO_ACL =
            new HashMap<List<String>, Map<Set<ResourcePattern>, Set<AccessControlEntry>>>() {{
                put(Collections.singletonList("--producer"), producerResourceToAcls(false));
                put(Arrays.asList("--producer", "--idempotent"), producerResourceToAcls(true));
                put(Collections.singletonList("--consumer"), CONSUMER_RESOURCE_TO_ACLS);
                put(Arrays.asList("--producer", "--consumer"),
                        CONSUMER_RESOURCE_TO_ACLS.entrySet().stream().map(entry -> {
                            Set<AccessControlEntry> value = new HashSet<>(entry.getValue());
                            value.addAll(producerResourceToAcls(false)
                                    .getOrDefault(entry.getKey(), Collections.emptySet()));
                            return new SimpleEntry<>(entry.getKey(), value);
                        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
                put(Arrays.asList("--producer", "--idempotent", "--consumer"),
                        CONSUMER_RESOURCE_TO_ACLS.entrySet().stream().map(entry -> {
                            Set<AccessControlEntry> value = new HashSet<>(entry.getValue());
                            value.addAll(producerResourceToAcls(true)
                                    .getOrDefault(entry.getKey(), Collections.emptySet()));
                            return new SimpleEntry<>(entry.getKey(), value);
                        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
            }};

    private final ClusterInstance cluster;

    AclCommandTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.ZK})
    public void testAclCliWithAuthorizer() {
        testAclCli(zkArgs());
    }

    @ClusterTests({
            @ClusterTest(types = {Type.ZK}),
            @ClusterTest(types = {Type.KRAFT}, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = STANDARD_AUTHORIZER)
            })
    })
    public void testAclCliWithAdminAPI() {
        testAclCli(adminArgs(Optional.empty()));
    }

    @ClusterTest(types = {Type.ZK})
    public void testProducerConsumerCliWithAuthorizer() {
        testProducerConsumerCli(zkArgs());
    }

    @ClusterTests({
            @ClusterTest(types = {Type.ZK}),
            @ClusterTest(types = {Type.KRAFT}, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = STANDARD_AUTHORIZER)
            })
    })
    public void testProducerConsumerCliWithAdminAPI() {
        testProducerConsumerCli(adminArgs(Optional.empty()));
    }

    @ClusterTests({
            @ClusterTest(types = {Type.ZK}),
            @ClusterTest(types = {Type.KRAFT}, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = STANDARD_AUTHORIZER)
            })
    })
    public void testAclCliWithClientId() {
        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        Level previousLevel = LogCaptureAppender.setClassLoggerLevel(AppInfoParser.class, Level.WARN);
        try {
            testAclCli(adminArgs(Optional.of(TestUtils.tempFile("client.id=my-client"))));
        } finally {
            LogCaptureAppender.setClassLoggerLevel(AppInfoParser.class, previousLevel);
            LogCaptureAppender.unregister(appender);
        }
        Option<LoggingEvent> warning = appender.getMessages().find(e -> e.getLevel() == Level.WARN &&
                e.getThrowableInformation() != null &&
                e.getThrowableInformation().getThrowable().getClass().getName().equals(InstanceAlreadyExistsException.class.getName()));
        assertFalse(warning.isDefined(), "There should be no warnings about multiple registration of mbeans");
    }

    @ClusterTest(types = {Type.ZK})
    public void testAclsOnPrefixedResourcesWithAuthorizer() {
        testAclsOnPrefixedResources(zkArgs());
    }

    @ClusterTests({
            @ClusterTest(types = {Type.ZK}),
            @ClusterTest(types = {Type.KRAFT}, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = STANDARD_AUTHORIZER)
            })
    })
    public void testAclsOnPrefixedResourcesWithAdminAPI() {
        testAclsOnPrefixedResources(adminArgs(Optional.empty()));
    }

    @ClusterTest(types = {Type.ZK})
    public void testInvalidAuthorizerProperty() {
        AclCommand.AuthorizerService aclCommandService = new AclCommand.AuthorizerService(
                AclAuthorizer.class.getName(),
                new AclCommandOptions(new String[]{"--authorizer-properties", "zookeeper.connect " + zkConnect()})
        );
        assertThrows(IllegalArgumentException.class, aclCommandService::listAcls);
    }

    @ClusterTest(types = {Type.ZK})
    public void testPatternTypesWithAuthorizer() {
        testPatternTypes(zkArgs());
    }

    @ClusterTests({
            @ClusterTest(types = {Type.ZK}),
            @ClusterTest(types = {Type.KRAFT}, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = STANDARD_AUTHORIZER)
            })
    })
    public void testPatternTypesWithAdminAPI() {
        testPatternTypes(adminArgs(Optional.empty()));
    }

    private void testProducerConsumerCli(List<String> cmdArgs) {
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
                    withAuthorizer(authorizer -> TestUtils.waitAndVerifyAcls(asScalaSet(resourcesToAclsEntry.getValue()), authorizer, resource, ANY));
                }
            }
            List<String> resourceCmd = new ArrayList<>(resourceCommand);
            resourceCmd.addAll(cmd);
            testRemove(cmdArgs, resourcesToAcls.keySet().stream().flatMap(Set::stream).collect(Collectors.toSet()), resourceCmd);
        }
    }

    private void testAclsOnPrefixedResources(List<String> cmdArgs) {
        List<String> cmd = Arrays.asList("--allow-principal", PRINCIPAL.toString(), "--producer", "--topic", "Test-", "--resource-pattern-type", "Prefixed");

        List<String> args = new ArrayList<>(cmdArgs);
        args.addAll(cmd);
        args.add(ADD);
        callMain(args);

        withAuthorizer(authorizer -> {
            AccessControlEntry writeAcl = new AccessControlEntry(PRINCIPAL.toString(), WILDCARD_HOST, WRITE, ALLOW);
            AccessControlEntry describeAcl = new AccessControlEntry(PRINCIPAL.toString(), WILDCARD_HOST, DESCRIBE, ALLOW);
            AccessControlEntry createAcl = new AccessControlEntry(PRINCIPAL.toString(), WILDCARD_HOST, CREATE, ALLOW);
            TestUtils.waitAndVerifyAcls(
                    asScalaSet(new HashSet<>(Arrays.asList(writeAcl, describeAcl, createAcl))),
                    authorizer,
                    new ResourcePattern(TOPIC, "Test-", PREFIXED),
                    ANY
            );
        });

        args = new ArrayList<>(cmdArgs);
        args.addAll(cmd);
        args.add("--remove");
        args.add("--force");
        callMain(args);

        withAuthorizer(authorizer -> {
            TestUtils.waitAndVerifyAcls(
                    asScalaSet(Collections.emptySet()),
                    authorizer,
                    new ResourcePattern(CLUSTER, "kafka-cluster", LITERAL),
                    ANY
            );
            TestUtils.waitAndVerifyAcls(
                    asScalaSet(Collections.emptySet()),
                    authorizer,
                    new ResourcePattern(TOPIC, "Test-", PREFIXED),
                    ANY
            );
        });
    }

    private static Map<Set<ResourcePattern>, Set<AccessControlEntry>> producerResourceToAcls(boolean enableIdempotence) {
        return new HashMap<Set<ResourcePattern>, Set<AccessControlEntry>>() {{
                put(TOPIC_RESOURCES, asJavaSet(AclCommand.getAcls(asScalaSet(USERS), ALLOW, asScalaSet(
                    new HashSet<>(Arrays.asList(WRITE, DESCRIBE, CREATE))), asScalaSet(HOSTS)))
                );
                put(TRANSACTIONAL_ID_RESOURCES, asJavaSet(AclCommand.getAcls(asScalaSet(USERS), ALLOW, asScalaSet(
                    new HashSet<>(Arrays.asList(WRITE, DESCRIBE))), asScalaSet(HOSTS)))
                );
                put(Collections.singleton(CLUSTER_RESOURCE), asJavaSet(AclCommand.getAcls(asScalaSet(USERS), ALLOW,
                    enableIdempotence ? asScalaSet(Collections.singleton(IDEMPOTENT_WRITE)) : asScalaSet(Collections.emptySet()), asScalaSet(HOSTS)))
                );
            }};
    }

    private List<String> adminArgs(Optional<File> commandConfig) {
        List<String> adminArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", cluster.bootstrapServers()));
        commandConfig.ifPresent(file -> adminArgs.addAll(Arrays.asList("--command-config", file.getAbsolutePath())));
        return adminArgs;
    }

    private Map.Entry<String, String> callMain(List<String> args) {
        AclCommand.main(args.toArray(new String[0]));
        return grabConsoleOutputAndError(() -> AclCommand.main(args.toArray(new String[0])));
    }

    private void testAclCli(List<String> cmdArgs) {
        for (Map.Entry<Set<ResourcePattern>, List<String>> entry : RESOURCE_TO_COMMAND.entrySet()) {
            Set<ResourcePattern> resources = entry.getKey();
            List<String> resourceCmd = entry.getValue();
            Set<AclPermissionType> permissionTypes = new HashSet<>(Arrays.asList(ALLOW, DENY));
            for (AclPermissionType permissionType : permissionTypes) {
                Tuple2<Set<AclOperation>, List<String>> operationToCmd = RESOURCE_TO_OPERATIONS.get(resources);
                Tuple2<Set<AccessControlEntry>, List<String>> aclToCommand = getAclToCommand(permissionType, operationToCmd._1);

                List<String> resultArgs = new ArrayList<>(cmdArgs);
                resultArgs.addAll(aclToCommand._2);
                resultArgs.addAll(resourceCmd);
                resultArgs.addAll(operationToCmd._2);
                resultArgs.add(ADD);

                Map.Entry<String, String> out = callMain(resultArgs);
                assertOutputContains("Adding ACLs", resources, resourceCmd, out.getKey());
                assertEquals("", out.getValue());

                for (ResourcePattern resource : resources) {
                    withAuthorizer(authorizer -> TestUtils.waitAndVerifyAcls(asScalaSet(aclToCommand._1), authorizer, resource, ANY));
                }

                resultArgs = new ArrayList<>(cmdArgs);
                resultArgs.add("--list");

                out = callMain(resultArgs);
                assertOutputContains("Current ACLs", resources, resourceCmd, out.getKey());
                assertEquals("", out.getValue());

                testRemove(cmdArgs, resources, resourceCmd);
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
                    ? Collections.singletonList("kafka-cluster")
                    : resourceCmd.stream().filter(s -> !s.startsWith("--")).collect(Collectors.toList());

            cmd.forEach(name -> {
                String expected = String.format("%s for resource `ResourcePattern(resourceType=%s, name=%s, patternType=LITERAL)`:", prefix, resourceType, name);
                assertTrue(output.contains(expected), "Substring " + expected + " not in output:\n" + output);
            });
        });
    }

    private void testPatternTypes(List<String> cmdArgs) {
        Exit.setExitProcedure((status, message) -> {
            if ((int) status == 1)
                throw new RuntimeException("Exiting command");
            else
                throw new AssertionError("Unexpected exit with status " + status);
        });
        try {
            Arrays.stream(PatternType.values()).sequential().forEach(patternType -> {
                List<String> addCmd = new ArrayList<>(cmdArgs);
                addCmd.addAll(Arrays.asList("--allow-principal", PRINCIPAL.toString(), "--producer", "--topic", "Test",
                        ADD, "--resource-pattern-type", patternType.toString()));
                verifyPatternType(addCmd, patternType.isSpecific());

                List<String> listCmd = new ArrayList<>(cmdArgs);
                listCmd.addAll(Arrays.asList("--topic", "Test", "--list", "--resource-pattern-type", patternType.toString()));
                verifyPatternType(listCmd, patternType != PatternType.UNKNOWN);

                List<String> removeCmd = new ArrayList<>(cmdArgs);
                removeCmd.addAll(Arrays.asList("--topic", "Test", "--force", "--remove", "--resource-pattern-type", patternType.toString()));
                verifyPatternType(removeCmd, patternType != PatternType.UNKNOWN);
            });
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private void verifyPatternType(List<String> cmd, boolean isValid) {
        if (isValid)
            callMain(cmd);
        else
            assertThrows(RuntimeException.class, () -> callMain(cmd));
    }

    private void testRemove(List<String> cmdArgs, Set<ResourcePattern> resources, List<String> resourceCmd) {
        List<String> args = new ArrayList<>(cmdArgs);
        args.addAll(resourceCmd);
        args.add("--remove");
        args.add("--force");
        Map.Entry<String, String> out = callMain(args);
        assertEquals("", out.getValue());
        for (ResourcePattern resource : resources) {
            withAuthorizer(authorizer -> TestUtils.waitAndVerifyAcls(asScalaSet(Collections.emptySet()), authorizer, resource, ANY));
        }
    }

    private Tuple2<Set<AccessControlEntry>, List<String>> getAclToCommand(
            AclPermissionType permissionType,
            Set<AclOperation> operations
    ) {
        return new Tuple2<>(
                asJavaSet(AclCommand.getAcls(asScalaSet(USERS), permissionType, asScalaSet(operations), asScalaSet(HOSTS))),
                getCmd(permissionType)
        );
    }

    private List<String> getCmd(AclPermissionType permissionType) {
        String principalCmd = permissionType == ALLOW ? "--allow-principal" : "--deny-principal";
        List<String> cmd = permissionType == ALLOW ? ALLOW_HOST_COMMAND : DENY_HOST_COMMAND;

        List<String> fullCmd = new ArrayList<>();
        for (KafkaPrincipal user : USERS) {
            fullCmd.addAll(cmd);
            fullCmd.addAll(Arrays.asList(principalCmd, user.toString()));
        }

        return fullCmd;
    }

    private void withAuthorizer(Consumer<Authorizer> consumer) {
        if (cluster.isKRaftTest()) {
            List<Authorizer> allAuthorizers = new ArrayList<>();
            allAuthorizers.addAll(asKraftCluster().brokers().values().stream().map(server -> server.authorizer().get()).collect(Collectors.toList()));
            allAuthorizers.addAll(asKraftCluster().controllers().values().stream().map(server -> server.authorizer().get()).collect(Collectors.toList()));
            allAuthorizers.forEach(consumer);
        } else {
            Properties props = new Properties();
            props.putAll(cluster.config().serverProperties());
            props.put("zookeeper.connect", zkConnect());
            try (AclAuthorizer auth = new AclAuthorizer()) {
                auth.configure(KafkaConfig.fromProps(props, false).originals());
                consumer.accept(auth);
            }
        }
    }

    /**
     * Capture both the console output and console error during the execution of the provided function.
     */
    private static Map.Entry<String, String> grabConsoleOutputAndError(Runnable runnable) {
        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBuf);
        PrintStream err = new PrintStream(errBuf);
        try {
            Console.withOut(out, () -> {
                Console.withErr(err, () -> {
                    runnable.run();
                    return null;
                });
                return null;
            });
        } finally {
            out.flush();
            err.flush();
        }

        return new AbstractMap.SimpleImmutableEntry<>(outBuf.toString(), errBuf.toString());
    }

    @SuppressWarnings("deprecation")
    private static <T> scala.collection.immutable.Set<T> asScalaSet(Set<T> javaSet) {
        return JavaConverters.asScalaSet(javaSet).toSet();
    }

    @SuppressWarnings("deprecation")
    private static <T> Set<T> asJavaSet(scala.collection.immutable.Set<T> scalaSet) {
        return JavaConverters.setAsJavaSet(scalaSet);
    }

    private String zkConnect() {
        return ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
    }

    private KafkaClusterTestKit asKraftCluster() {
        return ((RaftClusterInvocationContext.RaftClusterInstance) cluster).getUnderlying();
    }

    private List<String> zkArgs() {
        return Arrays.asList("--authorizer-properties", "zookeeper.connect=" + zkConnect());
    }
}
