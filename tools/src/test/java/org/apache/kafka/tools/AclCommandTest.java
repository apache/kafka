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

import kafka.security.authorizer.AclAuthorizer;
import kafka.server.KafkaServer;
import kafka.server.QuorumTestHarness;
import kafka.utils.LogCaptureAppender;
import kafka.utils.Logging;
import kafka.utils.TestUtils;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.tools.AclCommand.AuthorizerService;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import javax.management.InstanceAlreadyExistsException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
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
import java.util.stream.Stream;

import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.DELEGATION_TOKEN;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.common.resource.ResourceType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AclCommandTest extends QuorumTestHarness implements Logging {
    private List<KafkaServer> servers = new ArrayList<>();
    private static String wildcardHost = "*";

    private KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal("User:test2");
    private Set<KafkaPrincipal> users = new HashSet<>(Arrays.asList(
            SecurityUtils.parseKafkaPrincipal("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
            principal,
            SecurityUtils.parseKafkaPrincipal("User:CN=\\#User with special chars in CN : (\\, \\+ \\\" \\\\ \\< \\> \\; ')")
    ));
    private Set<String> hosts = new HashSet<>(Arrays.asList("host1", "host2"));
    private String[] allowHostCommand = {"--allow-host", "host1", "--allow-host", "host2"};
    private String[] denyHostCommand = {"--deny-host", "host1", "--deny-host", "host2"};

    private static ResourcePattern clusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL);
    private static Set<ResourcePattern> topicResources = new HashSet<>(Arrays.asList(
            new ResourcePattern(TOPIC, "test-1", LITERAL),
            new ResourcePattern(TOPIC, "test-2", LITERAL)
    ));
    private static Set<ResourcePattern> groupResources = new HashSet<>(Arrays.asList(
            new ResourcePattern(GROUP, "testGroup-1", LITERAL),
            new ResourcePattern(GROUP, "testGroup-2", LITERAL)
    ));
    private static Set<ResourcePattern> transactionalIdResources = new HashSet<>(Arrays.asList(
            new ResourcePattern(TRANSACTIONAL_ID, "t0", LITERAL),
            new ResourcePattern(TRANSACTIONAL_ID, "t1", LITERAL)
    ));
    private static Set<ResourcePattern> tokenResources = new HashSet<>(Arrays.asList(
            new ResourcePattern(DELEGATION_TOKEN, "token1", LITERAL),
            new ResourcePattern(DELEGATION_TOKEN, "token2", LITERAL)
    ));
    private static Set<ResourcePattern> userResources = new HashSet<>(Arrays.asList(
            new ResourcePattern(USER, "User:test-user1", LITERAL),
            new ResourcePattern(USER, "User:test-user2", LITERAL)
    ));

    private Map<Set<ResourcePattern>, String[]> resourceToCommand = new HashMap<Set<ResourcePattern>, String[]>() {{
        put(topicResources, new String[]{"--topic", "test-1", "--topic", "test-2"});
        put(Collections.singleton(clusterResource), new String[]{"--cluster"});
        put(groupResources, new String[]{"--group", "testGroup-1", "--group", "testGroup-2"});
        put(transactionalIdResources, new String[]{"--transactional-id", "t0", "--transactional-id", "t1"});
    }};

    private static Map<Set<ResourcePattern>, Set<AclOperation>> resourceToOperations = new HashMap<Set<ResourcePattern>, Set<AclOperation>>() {{
        put(topicResources, new HashSet<>(Arrays.asList(
                READ, WRITE, CREATE, DESCRIBE, AclOperation.DELETE,
                AclOperation.DESCRIBE_CONFIGS, AclOperation.ALTER_CONFIGS, AclOperation.ALTER)));
        put(Collections.singleton(clusterResource), new HashSet<>(Arrays.asList(CREATE, AclOperation.CLUSTER_ACTION,
                AclOperation.DESCRIBE_CONFIGS, AclOperation.ALTER_CONFIGS, IDEMPOTENT_WRITE,
                AclOperation.ALTER, DESCRIBE)));
        put(groupResources, new HashSet<>(Arrays.asList(READ, DESCRIBE, AclOperation.DELETE)));
        put(transactionalIdResources, new HashSet<>(Arrays.asList(DESCRIBE, WRITE)));
        put(tokenResources, new HashSet<>(Collections.singleton(DESCRIBE)));
        put(userResources, new HashSet<>(Arrays.asList(AclOperation.CREATE_TOKENS, AclOperation.DESCRIBE_TOKENS)));
    }};

    private Map<Set<ResourcePattern>, Set<AccessControlEntry>> ProducerResourceToAcls(boolean enableIdempotence) {
        return new HashMap<Set<ResourcePattern>, Set<AccessControlEntry>>() {{
            put(topicResources, AclCommand.getAcls(users, ALLOW, new HashSet<>(Arrays.asList(WRITE, DESCRIBE, CREATE)), hosts));
            put(transactionalIdResources, AclCommand.getAcls(users, ALLOW, new HashSet<>(Arrays.asList(WRITE, DESCRIBE)), hosts));
            put(Collections.singleton(clusterResource), AclCommand.getAcls(users, ALLOW, enableIdempotence ? Collections.singleton(IDEMPOTENT_WRITE) : Collections.emptySet(), hosts));
        }};
    }

    private Map<Set<ResourcePattern>, Set<AccessControlEntry>> consumerResourceToAcls = new HashMap<Set<ResourcePattern>, Set<AccessControlEntry>>() {{
        put(topicResources, AclCommand.getAcls(users, ALLOW, new HashSet<>(Arrays.asList(READ, DESCRIBE)), hosts));
        put(groupResources, AclCommand.getAcls(users, ALLOW, Collections.singleton(READ), hosts));
    }};

    Map<String[], Map<Set<ResourcePattern>, Set<AccessControlEntry>>> cmdToResourcesToAcl = new HashMap<String[], Map<Set<ResourcePattern>, Set<AccessControlEntry>>>() {{
        put(new String[]{"--producer"}, ProducerResourceToAcls(false));
        put(new String[]{"--producer", "--idempotent"}, ProducerResourceToAcls(true));
        put(new String[]{"--consumer"}, consumerResourceToAcls);
        put(new String[]{"--producer", "--consumer"}, consumerResourceToAcls.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> {
                    Set<AccessControlEntry> acls = new HashSet<>(entry.getValue());
                    acls.addAll(ProducerResourceToAcls(false).getOrDefault(entry.getKey(), Collections.emptySet()));
                    return acls;
                })));
        put(new String[]{"--producer", "--idempotent", "--consumer"}, consumerResourceToAcls.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> {
                    Set<AccessControlEntry> acls = new HashSet<>(entry.getValue());
                    acls.addAll(ProducerResourceToAcls(true).getOrDefault(entry.getKey(), Collections.emptySet()));
                    return acls;
                }))); }};

    private Properties brokerProps = new Properties();
    private String[] zkArgs = new String[]{};
    private String[] adminArgs = new String[]{};

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        brokerProps =  TestUtils.createBrokerConfig(0, zkConnect(), true, true,
                0, scala.Option.empty(), scala.Option.empty(), scala.Option.empty(), true,
                false, 0, false, 0, false, 0,
                scala.Option.empty(), 1, false, 1, (short) 1, false);
        brokerProps.put(kafka.server.KafkaConfig$.MODULE$.AuthorizerClassNameProp(), AclAuthorizer.class.getName());
        brokerProps.put(AclAuthorizer.SuperUsersProp(), "User:ANONYMOUS");

        zkArgs = new String[]{"--authorizer-properties", "zookeeper.connect=" + zkConnect()};
    }

    @AfterEach
    @Override
    public void tearDown() {
        TestUtils.shutdownServers(scala.collection.JavaConverters.asScala(servers), true);
        super.tearDown();
    }

    @Test
    public void testAclCliWithAuthorizer() {
        testAclCli(zkArgs);
    }

    @Test
    public void testAclCliWithAdminAPI() {
        createServer(Optional.empty());
        testAclCli(adminArgs);
    }

    private void createServer(Optional<File> commandConfig) {
        kafka.server.KafkaConfig kafkaConfig = kafka.server.KafkaConfig$.MODULE$.fromProps(brokerProps);
        servers = Collections.singletonList(TestUtils.createServer(kafkaConfig, Time.SYSTEM));
        ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);

        String bootstrapServers = TestUtils.bootstrapServers(scala.collection.JavaConverters.asScala(servers), listenerName);
        List<String> adminArgsList = Arrays.asList("--bootstrap-server", bootstrapServers);
        if (commandConfig.isPresent()) {
            adminArgsList.add("--command-config");
            adminArgsList.add(commandConfig.get().getAbsolutePath());
        }
        this.adminArgs = adminArgsList.stream().toArray(String[]::new);
    }

    private void testAclCli(String[] cmdArgs) {
        resourceToCommand.forEach((resources, resourceCmd) -> {

            Arrays.asList(ALLOW, DENY).forEach(permissionType -> {
                Set<AclOperation> operationToCmd = resourceToOperations.get(resources);
                Map.Entry<Set<AccessControlEntry>, String[]> aclToCommand = getAclToCommand(permissionType, operationToCmd);
                Set<AccessControlEntry> acls = aclToCommand.getKey();
                String[] cmd = aclToCommand.getValue();

                Map.Entry<String, String> result = callMain(Stream.of(cmdArgs, cmd, resourceCmd, operationToCmd).toArray(String[]::new));
                String addOut = result.getKey();
                String addErr = result.getValue();
                assertOutputContains("Adding ACLs", resources, resourceCmd, addOut);
                assertOutputContains("Current ACLs", resources, resourceCmd, addOut);
                assertEquals("", addErr);

                for (ResourcePattern resource: resources) {
                    withAuthorizer(authorizer -> TestUtils.waitAndVerifyAcls(
                            scala.collection.JavaConverters.asScala(acls).toSet(), authorizer, resource,
                            AccessControlEntryFilter.ANY));
                }
                Map.Entry<String, String> listingResult = callMain(Stream.of(cmdArgs, new String[]{"--list"}).toArray(String[]::new));
                String listOut = listingResult.getKey();
                String listErr = listingResult.getValue();

                assertOutputContains("Current ACLs", resources, resourceCmd, listOut);
                assertEquals("", listErr);
                testRemove(cmdArgs, resources, resourceCmd);
            });
        });
    }

    private AbstractMap.SimpleImmutableEntry<Set<AccessControlEntry>, String[]> getAclToCommand(AclPermissionType permissionType, Set<AclOperation> operations) {
        return new AbstractMap.SimpleImmutableEntry<>(AclCommand.getAcls(users, permissionType, operations, hosts), getCmd(permissionType));
    }

    private String[] getCmd(AclPermissionType permissionType) {
        String principalCmd = permissionType == ALLOW ? "--allow-principal" : "--deny-principal";
        String[] cmd = permissionType == ALLOW ? allowHostCommand : denyHostCommand;
        List<String> userACLCmd = users.stream()
                .map(user -> Arrays.asList(principalCmd, user.toString()))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return Stream.concat(Arrays.stream(cmd), userACLCmd.stream())
                .toArray(String[]::new);
    }

    private void testRemove(String[] cmdArgs, Set<ResourcePattern> resources, String[] resourceCmd) {
        Map.Entry<String, String> result = callMain(Stream.of(cmdArgs, resourceCmd, new String[]{"--remove", "--force"}).toArray(String[]::new));
        assertEquals("", result.getKey());
        assertEquals("", result.getValue());

        for (ResourcePattern resource: resources) {
            withAuthorizer(authorizer -> TestUtils.waitAndVerifyAcls(scala.collection.JavaConverters.asScala(Collections.emptySet()).toSet(),
                    authorizer, resource, AccessControlEntryFilter.ANY));
        }

    }

    private AbstractMap.SimpleImmutableEntry<String, String> callMain(String[] args) {
        return new AbstractMap.SimpleImmutableEntry<>(ToolsTestUtils.captureStandardOut(() -> AclCommand.main(args)),
                ToolsTestUtils.captureStandardErr(() -> AclCommand.main(args)));
    }

    private void assertOutputContains(String prefix, Set<ResourcePattern> resources, String[] resourceCmd, String output) {
        resources.forEach(resource -> {
            String resourceType = resource.resourceType().toString();
            String[] args = (resource == clusterResource) ?
                    new String[]{"kafka-cluster"} :
                    Arrays.stream(resourceCmd).filter(cmd -> !cmd.startsWith("--")).toArray(String[]::new);
            for (String arg: args) {
                String expected = String.format("%s for resource `ResourcePattern(resourceType=%s, name=%s, patternType=LITERAL)`:",
                        prefix, resourceType, arg);
                assertTrue(output.contains(expected));
            }
        });
    }

    private void withAuthorizer(Consumer<Authorizer> f) {
        kafka.server.KafkaConfig kafkaConfig = kafka.server.KafkaConfig$.MODULE$.fromProps(brokerProps, false);
        AclAuthorizer authZ = new AclAuthorizer();
        try {
            authZ.configure(kafkaConfig.originals());
            f.accept(authZ);
        } finally {
            authZ.close();
        }
    }

    @Test
    public void testProducerConsumerCliWithAuthorizer() {
        testProducerConsumerCli(zkArgs);
    }

    @Test
    public void testProducerConsumerCliWithAdminAPI() {
        createServer(Optional.empty());
        testProducerConsumerCli(adminArgs);
    }

    @Test
    public void testAclCliWithClientId() throws FileNotFoundException {
        File adminClientConfig = TestUtils.tempFile();
        PrintWriter pw = new PrintWriter(adminClientConfig);
        pw.println("client.id=my-client");
        pw.close();

        createServer(Optional.of(adminClientConfig));

        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        Level previousLevel = LogCaptureAppender.setClassLoggerLevel(AppInfoParser.class, Level.WARN);
        try {
            testAclCli(adminArgs);
        } finally {
            LogCaptureAppender.setClassLoggerLevel(AppInfoParser.class, previousLevel);
            LogCaptureAppender.unregister(appender);
        }
        scala.Option<LoggingEvent> warning = appender.getMessages().find(e -> e.getLevel() == Level.WARN &&
                e.getThrowableInformation() != null &&
                e.getThrowableInformation().getThrowable().getClass().getName() == InstanceAlreadyExistsException.class.getName());
        assertFalse(warning.isDefined(), "There should be no warnings about multiple registration of mbeans");

    }

    public void testProducerConsumerCli(String[] cmdArgs) {
        for (Map.Entry<String[], Map<Set<ResourcePattern>, Set<AccessControlEntry>>> entry : cmdToResourcesToAcl.entrySet()) {
            String[] cmd = entry.getKey();
            Map<Set<ResourcePattern>, Set<AccessControlEntry>> resourcesToAcls = entry.getValue();
            String[] resourceCommand = resourcesToAcls.keySet().stream()
                    .map(resourceToCommand::get)
                    .flatMap(Arrays::stream)
                    .toArray(String[]::new);

            callMain(Stream.of(cmdArgs, getCmd(ALLOW), resourceCommand, cmd, new String[]{"--add"}).toArray(String[]::new));

            for (Map.Entry<Set<ResourcePattern>, Set<AccessControlEntry>> resourceToAcls : resourcesToAcls.entrySet()) {
                for (ResourcePattern resource : resourceToAcls.getKey()) {
                    withAuthorizer(authorizer -> TestUtils.waitAndVerifyAcls(scala.collection.JavaConverters.asScala(resourceToAcls.getValue()).toSet(),
                            authorizer, resource, AccessControlEntryFilter.ANY));
                }
            }

            testRemove(cmdArgs, resourcesToAcls.keySet().stream().flatMap(Set::stream).collect(Collectors.toSet()),
                    Stream.of(resourceCommand, cmd).toArray(String[]::new));
        }
    }

    @Test
    public void testAclsOnPrefixedResourcesWithAuthorizer() {
        testAclsOnPrefixedResources(zkArgs);
    }

    @Test
    public void testAclsOnPrefixedResourcesWithAdminAPI() {
        createServer(Optional.empty());
        testAclsOnPrefixedResources(adminArgs);
    }

    private void testAclsOnPrefixedResources(String[] cmdArgs) {
        String[] cmd = new String[]{"--allow-principal", principal.toString(), "--producer", "--topic", "Test-", "--resource-pattern-type", "Prefixed"};
        callMain(Stream.of(cmdArgs, cmd, new String[]{"--add"}).toArray(String[]::new));

        withAuthorizer(authorizer -> {
            AccessControlEntry writeAcl = new AccessControlEntry(principal.toString(), wildcardHost, WRITE, ALLOW);
            AccessControlEntry describeAcl = new AccessControlEntry(principal.toString(), wildcardHost, DESCRIBE, ALLOW);
            AccessControlEntry createAcl = new AccessControlEntry(principal.toString(), wildcardHost, CREATE, ALLOW);
            ResourcePattern resource = new ResourcePattern(TOPIC, "Test-", PatternType.PREFIXED);
            Set<AccessControlEntry> accessControlEntries = new HashSet<>(Arrays.asList(writeAcl, describeAcl, createAcl));
            TestUtils.waitAndVerifyAcls(scala.collection.JavaConverters.asScala(accessControlEntries).toSet(), authorizer, resource, AccessControlEntryFilter.ANY);
        });
    }

    @Test
    public void testInvalidAuthorizerProperty() {
        String[] args = Stream.of(new String[]{"--authorizer-properties", "zookeeper.connect "}, zkConnect()).toArray(String[]::new);

        AuthorizerService aclCommandService = new AuthorizerService(AclAuthorizer.class.getName(),
                new AclCommand.AclCommandOptions(args));
        assertThrows(IllegalArgumentException.class, () -> aclCommandService.listAcls());
    }

    @Test
    public void testPatternTypes() {
        Exit.setExitProcedure((status, result) -> {
            if (status == 1) {
                throw new RuntimeException("Exiting command");
            } else {
                throw new AssertionError(String.format("Unexpected exit with status %s", status));
            }
        });
        try {
            for (PatternType patternType: PatternType.values()) {
                String[] addCmd = Stream.concat(
                        Arrays.stream(zkArgs),
                        Arrays.stream(new String[]{"--allow-principal", principal.toString(), "--producer", "--topic", "Test", "--add", "--resource-pattern-type", patternType.toString()})
                ).toArray(String[]::new);
                verifyPatternType(addCmd, patternType.isSpecific());

                String[] listCmd = Stream.concat(
                        Arrays.stream(zkArgs),
                        Arrays.stream(new String[]{"--topic", "Test", "--list", "--resource-pattern-type", patternType.toString()})
                ).toArray(String[]::new);
                verifyPatternType(listCmd, patternType != PatternType.UNKNOWN);

                String[] removeCmd = Stream.concat(
                        Arrays.stream(zkArgs),
                        Arrays.stream(new String[]{"--topic", "Test", "--force", "--remove", "--resource-pattern-type", patternType.toString()})
                ).toArray(String[]::new);
                verifyPatternType(removeCmd, patternType != PatternType.UNKNOWN);
            }
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private void verifyPatternType(String[] cmd, Boolean isValid) {
        if (isValid)
            callMain(cmd);
        else
            assertThrows(RuntimeException.class, () -> callMain(cmd));
    }
}

