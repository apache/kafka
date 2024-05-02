package kafka.admin;

import kafka.test.ClusterInstance;
import kafka.test.annotation.*;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.security.authorizer.AclEntry;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Console$;
import scala.collection.JavaConverters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.concat;
import static org.apache.kafka.common.acl.AclOperation.*;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.*;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.SUPER_USERS_CONFIG;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.*;
import static scala.collection.JavaConverters.setAsJavaSet;

@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults
@Tag("integration")
public class AclCommandIntegrationTest {
    private final ClusterInstance cluster;

    private static final String AUTHORIZER = ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
    private static final String ACL_AUTHORIZER = "kafka.security.authorizer.AclAuthorizer";
    private static final String STANDARD_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";
    private static final String SUPER_USERS = "super.users";
    private static final String USER_ANONYMOUS = "User:ANONYMOUS";

    private static final KafkaPrincipal PRINCIPAL = SecurityUtils.parseKafkaPrincipal("User:test2");
    private static final Set<KafkaPrincipal> USERS = setOf(SecurityUtils.parseKafkaPrincipal("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
            PRINCIPAL, SecurityUtils.parseKafkaPrincipal("User:CN=\\#User with special chars in CN : (\\, \\+ \\\" \\ \\< \\> \\; ')"));
    private static final Set<String> HOSTS = setOf("host1", "host2");
    private static final List<String> ALLOW_HOST_COMMAND = asList("--allow-host", "host1", "--allow-host", "host2");
    private static final List<String> DENY_HOST_COMMAND = asList("--deny-host", "host1", "--deny-host", "host2");

    private static final ResourcePattern CLUSTER_RESOURCE = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL);
    private static final Set<ResourcePattern> TOPIC_RESOURCES = setOf(new ResourcePattern(TOPIC, "test-1", LITERAL), new ResourcePattern(TOPIC, "test-2", LITERAL));
    private static final Set<ResourcePattern> GROUP_RESOURCES = setOf(new ResourcePattern(GROUP, "testGroup-1", LITERAL), new ResourcePattern(GROUP, "testGroup-2", LITERAL));
    private static final Set<ResourcePattern> TRANSACTIONAL_ID_RESOURCES = setOf(new ResourcePattern(TRANSACTIONAL_ID, "t0", LITERAL), new ResourcePattern(TRANSACTIONAL_ID, "t1", LITERAL));
    private static final Set<ResourcePattern> TOKEN_RESOURCES = setOf(new ResourcePattern(DELEGATION_TOKEN, "token1", LITERAL), new ResourcePattern(DELEGATION_TOKEN, "token2", LITERAL));
    private static final Set<ResourcePattern> USER_RESOURCES = setOf(new ResourcePattern(USER, "User:test-user1", LITERAL), new ResourcePattern(USER, "User:test-user2", LITERAL));

    public static final Map<Set<ResourcePattern>, List<String>> RESOURCE_TO_COMMAND = new HashMap<>();
    static {
        RESOURCE_TO_COMMAND.put(TOPIC_RESOURCES, asList("--topic", "test-1", "--topic", "test-2"));
        RESOURCE_TO_COMMAND.put(singleton(CLUSTER_RESOURCE), singletonList("--cluster"));
        RESOURCE_TO_COMMAND.put(GROUP_RESOURCES, asList("--group", "testGroup-1", "--group", "testGroup-2"));
        RESOURCE_TO_COMMAND.put(TRANSACTIONAL_ID_RESOURCES, asList("--transactional-id", "t0", "--transactional-id", "t1"));
        RESOURCE_TO_COMMAND.put(TOKEN_RESOURCES, asList("--delegation-token", "token1", "--delegation-token", "token2"));
        RESOURCE_TO_COMMAND.put(USER_RESOURCES, asList("--user-principal", "User:test-user1", "--user-principal", "User:test-user2"));
    }

    public static final Map<Set<ResourcePattern>, SimpleEntry<Set<AclOperation>, List<String>>> RESOURCE_TO_OPERATIONS = new HashMap<>();
    static {
        RESOURCE_TO_OPERATIONS.put(TOPIC_RESOURCES, new SimpleEntry<>(setOf(READ, WRITE, CREATE, DESCRIBE, DELETE, DESCRIBE_CONFIGS, ALTER_CONFIGS, ALTER),
                asList("--operation", "Read", "--operation", "Write", "--operation", "Create", "--operation", "Describe", "--operation", "Delete",
                        "--operation", "DescribeConfigs", "--operation", "AlterConfigs", "--operation", "Alter")));
        RESOURCE_TO_OPERATIONS.put(singleton(CLUSTER_RESOURCE), new SimpleEntry<>(setOf(CREATE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALTER, DESCRIBE),
                asList("--operation", "Create", "--operation", "ClusterAction", "--operation", "DescribeConfigs",
                        "--operation", "AlterConfigs", "--operation", "IdempotentWrite", "--operation", "Alter", "--operation", "Describe")));
        RESOURCE_TO_OPERATIONS.put(GROUP_RESOURCES, new SimpleEntry<>(setOf(READ, DESCRIBE, DELETE),
                asList("--operation", "Read", "--operation", "Describe", "--operation", "Delete")));
        RESOURCE_TO_OPERATIONS.put(TRANSACTIONAL_ID_RESOURCES, new SimpleEntry<>(setOf(DESCRIBE, WRITE),
                asList("--operation", "Describe", "--operation", "Write")));
        RESOURCE_TO_OPERATIONS.put(TOKEN_RESOURCES, new SimpleEntry<>(singleton(DESCRIBE),
                asList("--operation", "Describe")));
        RESOURCE_TO_OPERATIONS.put(USER_RESOURCES, new SimpleEntry<>(setOf(CREATE_TOKENS, DESCRIBE_TOKENS),
                asList("--operation", "CreateTokens", "--operation", "DescribeTokens")));
    }

    private static Map<Set<ResourcePattern>, Set<AccessControlEntry>> producerResourceToAcls(boolean enableIdempotence) {
        Map<Set<ResourcePattern>, Set<AccessControlEntry>> map = new HashMap<>();
        map.put(TOPIC_RESOURCES, setAsJavaSet(AclCommand.getAcls(scalaSet(USERS), ALLOW, scalaSet(asList(WRITE, DESCRIBE, CREATE)), scalaSet(HOSTS))));
        map.put(TRANSACTIONAL_ID_RESOURCES, setAsJavaSet(AclCommand.getAcls(scalaSet(USERS), ALLOW, scalaSet(asList(WRITE, DESCRIBE)), scalaSet(HOSTS))));
        map.put(singleton(CLUSTER_RESOURCE),
                setAsJavaSet(AclCommand.getAcls(scalaSet(USERS), ALLOW,
                        scalaSet(enableIdempotence ? singleton(IDEMPOTENT_WRITE) : emptySet()),
                        scalaSet(HOSTS))));
        return map;
    }

    private static final Map<Set<ResourcePattern>, Set<AccessControlEntry>> consumerResourceToAcls = new HashMap<>();
    static {
        consumerResourceToAcls.put(TOPIC_RESOURCES, setAsJavaSet(AclCommand.getAcls(scalaSet(USERS), ALLOW, scalaSet(asList(READ, DESCRIBE)), scalaSet(HOSTS))));
        consumerResourceToAcls.put(GROUP_RESOURCES, setAsJavaSet(AclCommand.getAcls(scalaSet(USERS), ALLOW, scalaSet(singleton(READ)), scalaSet(HOSTS))));
    }

    private static final Map<List<String>, Map<Set<ResourcePattern>, Set<AccessControlEntry>>> cmdToResourcesToAcl = new HashMap<>();
    static {
        cmdToResourcesToAcl.put(singletonList("--producer"), producerResourceToAcls(false));
        cmdToResourcesToAcl.put(asList("--producer", "--idempotent"), producerResourceToAcls(true));
        cmdToResourcesToAcl.put(singletonList("--consumer"), consumerResourceToAcls);
        cmdToResourcesToAcl.put(asList("--producer", "--consumer"),
                consumerResourceToAcls.entrySet().stream().collect(toMap(
                        Entry::getKey,
                        e -> concat(e.getValue().stream(),
                                producerResourceToAcls(false).getOrDefault(e.getKey(), emptySet()).stream())
                                .collect(toSet()))));
        cmdToResourcesToAcl.put(asList("--producer", "--idempotent", "--consumer"),
                consumerResourceToAcls.entrySet().stream().collect(toMap(
                        Entry::getKey,
                        e -> concat(e.getValue().stream(),
                                producerResourceToAcls(true).getOrDefault(e.getKey(), emptySet()).stream())
                                .collect(toSet()))));
    }

    public AclCommandIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTests({
            @ClusterTest(types = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = ACL_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS, value = USER_ANONYMOUS)
            }),
            @ClusterTest(types = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = STANDARD_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = USER_ANONYMOUS)
            })
    })
    public void testAclCliWithAdminAPI() {
        testAclCli(emptyList());
    }

    private SimpleEntry<String, String> callMain(String[] args) {
        return grabConsoleOutputAndError(() -> AclCommand.main(args));
    }

    private void testAclCli(List<String> extraArgs) {
        for (Entry<Set<ResourcePattern>, List<String>> e : RESOURCE_TO_COMMAND.entrySet()) {
            for (AclPermissionType permissionType : asList(ALLOW, DENY)) {
                final Set<ResourcePattern> resources = e.getKey();
                final List<String> resourceCmd = e.getValue();

                SimpleEntry<Set<AclOperation>, List<String>> operationToCmd = RESOURCE_TO_OPERATIONS.get(resources);
                SimpleEntry<scala.collection.Set<AccessControlEntry>, List<String>> aclsToCmd = getAclToCommand(permissionType, operationToCmd.getKey());
                Set<AccessControlEntry> acls = setAsJavaSet(aclsToCmd.getKey());
                List<String> cmd = aclsToCmd.getValue();

                SimpleEntry<String, String> outToErr = callMain(toArray(
                        quorumArgs(), extraArgs, cmd, resourceCmd, operationToCmd.getValue(), singletonList("--add")));

                String addOut = outToErr.getKey();
                String addErr = outToErr.getValue();

                assertOutputContains("Adding ACLs", resources, resourceCmd, addOut);
                // In case of Kraft, it can take some time for the ACL metadata to propagate to all brokers, so the immediate
                // output may be not correct. Eventually, ACLs will be propagated - see below verifyAclWithAdmin
                if (!cluster.isKRaftTest()) {
                    assertOutputContains("Current ACLs", resources, resourceCmd, addOut);
                }
                assertEquals("", addErr);

                for (ResourcePattern resource : resources) {
                    verifyAclWithAdmin(acls, resource);
                }

                SimpleEntry<String, String> listOutToErr = callMain(
                        toArray(quorumArgs(), extraArgs, singletonList("--list")));

                String listOut = listOutToErr.getKey();
                String listErr = listOutToErr.getValue();

                assertOutputContains("Current ACLs", resources, resourceCmd, listOut);
                assertEquals("", listErr);

                testRemove(quorumArgs(), resources, resourceCmd);
            }
        }
    }

    @ClusterTests({
            @ClusterTest(types = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = ACL_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS, value = USER_ANONYMOUS)
            }),
            @ClusterTest(types = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = STANDARD_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = USER_ANONYMOUS)
            })
    })
    public void testProducerConsumerCliWithAdminAPI() {
        testProducerConsumerCli();
    }

    private void testProducerConsumerCli() {
        cmdToResourcesToAcl.forEach((cmd, resourcesToAcls) -> {
            List<String> resourceCommand = new ArrayList<>();
            resourcesToAcls.keySet().stream().map(RESOURCE_TO_COMMAND::get).forEach(resourceCommand::addAll);

            callMain(toArray(quorumArgs(), getCmd(ALLOW), resourceCommand, cmd, singletonList("--add")));

            resourcesToAcls.forEach((resources, acls) -> {
                for (ResourcePattern resource : resources) {
                    verifyAclWithAdmin(acls, resource);
                }
            });

            testRemove(quorumArgs(), resourcesToAcls.keySet().stream().flatMap(Collection::stream).collect(toSet()),
                    joinLists(resourceCommand, cmd));
        });
    }

    @ClusterTests({
            @ClusterTest(types = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = ACL_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS, value = USER_ANONYMOUS)
            }),
            @ClusterTest(types = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = STANDARD_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = USER_ANONYMOUS)
            })
    })
    public void testAclCliWithClientId() throws IOException {
        File adminClientConfig = tempFile("client.id=my-client");

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(AppInfoParser.class)) {
            testAclCli(asList("--command-config", adminClientConfig.getAbsolutePath()));

            int warningCount = (int) appender.getEvents().stream().filter(e -> "WARN".equals(e.getLevel()) &&
                    e.getThrowableInfo().isPresent() &&
                    e.getThrowableInfo().get().contains("InstanceAlreadyExistsException")).count();
            assertFalse(warningCount > 0, "There should be no warnings about multiple registration of mbeans");
        }
    }

    @ClusterTests({
            @ClusterTest(types = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = ACL_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS, value = USER_ANONYMOUS)
            }),
            @ClusterTest(types = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = STANDARD_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = USER_ANONYMOUS)
            })
    })
    public void testAclsOnPrefixedResourcesWithAdminAPI() throws IOException {
        testAclsOnPrefixedResources();
    }

    private void testAclsOnPrefixedResources() {
        List<String> cmd = asList("--allow-principal", PRINCIPAL.toString(), "--producer", "--topic", "Test-", "--resource-pattern-type", "Prefixed");

        callMain(toArray(quorumArgs(), cmd, singletonList("--add")));

        AccessControlEntry writeAcl = new AccessControlEntry(PRINCIPAL.toString(), AclEntry.WILDCARD_HOST, WRITE, ALLOW);
        AccessControlEntry describeAcl = new AccessControlEntry(PRINCIPAL.toString(), AclEntry.WILDCARD_HOST, DESCRIBE, ALLOW);
        AccessControlEntry createAcl = new AccessControlEntry(PRINCIPAL.toString(), AclEntry.WILDCARD_HOST, CREATE, ALLOW);

        verifyAclWithAdmin(setOf(writeAcl, describeAcl, createAcl),
                new ResourcePattern(TOPIC, "Test-", PREFIXED));

        callMain(toArray(quorumArgs(), cmd, asList("--remove", "--force")));

        verifyAclWithAdmin(emptySet(), new ResourcePattern(CLUSTER, "kafka-cluster", LITERAL));
        verifyAclWithAdmin(emptySet(), new ResourcePattern(TOPIC, "Test-", PREFIXED));
    }

    @ClusterTests({
            @ClusterTest(types = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = ACL_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS, value = USER_ANONYMOUS)
            }),
            @ClusterTest(types = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = AUTHORIZER, value = STANDARD_AUTHORIZER),
                    @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = USER_ANONYMOUS)
            })
    })
    public void testPatternTypesWithAdminAPI() throws IOException {
        testPatternTypes();
    }

    private void testPatternTypes() {
        List<String> cmdArgs = quorumArgs();

        Exit.setExitProcedure((status, __) -> {
            if (status == 1)
                throw new RuntimeException("Exiting command");
            else
                throw new AssertionError(String.format("Unexpected exit with status %d", status));
        });

        try {
            for (PatternType patternType : PatternType.values()) {
                List<String> addCmd = joinLists(cmdArgs, asList(
                        "--allow-principal", PRINCIPAL.toString(), "--producer", "--topic", "Test",
                        "--add", "--resource-pattern-type", patternType.toString()));
                verifyPatternType(addCmd, patternType.isSpecific());

                List<String> listCmd = joinLists(cmdArgs, asList(
                        "--topic", "Test", "--list", "--resource-pattern-type", patternType.toString()));
                verifyPatternType(listCmd, patternType != PatternType.UNKNOWN);

                List<String> removeCmd = joinLists(cmdArgs, asList(
                        "--topic", "Test", "--force", "--remove", "--resource-pattern-type", patternType.toString()));
                verifyPatternType(removeCmd, patternType != PatternType.UNKNOWN);
            }
        } finally {
            Exit.resetExitProcedure();
        }
    }

    void verifyPatternType(List<String> cmdList, boolean isValid) {
        String[] cmd = cmdList.stream().toArray(String[]::new);
        if (isValid)
            callMain(cmd);
        else
            assertThrows(RuntimeException.class, () -> callMain(cmd));
    }

    private List<String> joinLists(List<String> list1, List<String> list2) {
        return Stream.concat(list1.stream(), list2.stream()).collect(toList());
    }

    private static <T> Set<T> setOf(T... a) {
        return new HashSet<>(Arrays.asList(a));
    }

    private String[] toArray(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toArray(String[]::new);
    }

    private void assertOutputContains(String prefix, Set<ResourcePattern> resources, List<String> resourceCmd, String output) {
        resources.forEach(resource -> {
            String resourceType = resource.resourceType().toString();
            List<String> resourceNames = (resource == CLUSTER_RESOURCE) ? Collections.singletonList("kafka-cluster") :
                    resourceCmd.stream().filter(cmd -> !cmd.startsWith("--")).collect(toList());
            for (String name : resourceNames) {
                String expected = String.format("%s for resource `ResourcePattern(resourceType=%s, name=%s, patternType=LITERAL)`:",
                        prefix, resourceType, name);
                assertTrue(output.contains(expected), String.format("Substring %s not in output:\n%s", expected, output));
            }
        });
    }

    private List<String> quorumArgs() {
        return Arrays.asList("--bootstrap-server", cluster.bootstrapServers());
    }

    private SimpleEntry<scala.collection.Set<AccessControlEntry>, List<String>> getAclToCommand(AclPermissionType permissionType, Set<AclOperation> operations) {
        return new SimpleEntry(AclCommand.getAcls(scalaSet(USERS), permissionType, scalaSet(operations), scalaSet(HOSTS)),
                getCmd(permissionType));
    }

    private List<String> getCmd(AclPermissionType permissionType) {
        String principalCmd = (permissionType == ALLOW) ? "--allow-principal" : "--deny-principal";
        List<String> cmd = (permissionType == ALLOW) ? ALLOW_HOST_COMMAND : DENY_HOST_COMMAND;
        List<String> command = new ArrayList<>(cmd);
        for (KafkaPrincipal user : USERS) {
            command.add(principalCmd);
            command.add(user.toString());
        }
        return command;
    }

    @SuppressWarnings({"deprecation"})
    private static <T> scala.collection.immutable.Set<T> scalaSet(Collection<T> seq) {
        return JavaConverters.asScalaIteratorConverter(seq.iterator()).asScala().toSet();
    }

    private void testRemove(List<String> cmdArgs, Set<ResourcePattern> resources, List<String> resourceCmd) {
        SimpleEntry<String, String> outToErr = callMain(toArray(cmdArgs, resourceCmd, asList("--remove", "--force")));
        String out = outToErr.getKey();
        String err = outToErr.getValue();
        // In case of Kraft, it can take some time for the ACL metadata to propagate to all brokers, so the immediate
        // output may be not correct. Eventually, ACLs will be propagated - see below verifyAclWithAdmin
        if (!cluster.isKRaftTest()) {
            assertEquals("", out);
        }
        assertEquals("", err);
        for (ResourcePattern resource : resources) {
            verifyAclWithAdmin(emptySet(), resource);
        }
    }

    private void verifyAclWithAdmin(Set<AccessControlEntry> expectedAcls, ResourcePattern resource) {
        try (Admin admin = cluster.createAdminClient()) {
            AclBindingFilter filter = new AclBindingFilter(resource.toFilter(), AccessControlEntryFilter.ANY);

            TestUtils.waitForCondition(() -> admin.describeAcls(filter).values().get().stream().
                    map(AclBinding::entry).collect(toSet()).containsAll(expectedAcls), 45000, () -> {
                try {
                    return "expected acls:" + aclsToString(expectedAcls) + "but got:" + aclsToString(
                            admin.describeAcls(filter).values().get().stream().map(AclBinding::entry).collect(toSet()));
                } catch (Exception e) {
                }
                return "";
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String aclsToString(Collection<AccessControlEntry> acls) {
        String newLine = System.lineSeparator();
        return acls.stream().map(AccessControlEntry::toString).collect(
                Collectors.joining(newLine + "\t", newLine + "\t", newLine));
    }

    private static SimpleEntry<String, String> grabConsoleOutputAndError(Runnable runnable) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream tempOutputStream = new PrintStream(outputStream);
        Console$.MODULE$.setOutDirect(tempOutputStream);

        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
        PrintStream tempErrorStream = new PrintStream(errorStream);
        Console$.MODULE$.setErrDirect(tempErrorStream);

        try {
            runnable.run();
            return new SimpleEntry<>(outputStream.toString().trim(),
                    errorStream.toString().trim());
        } finally {
            Console$.MODULE$.setOutDirect(System.out);
            Console$.MODULE$.setErrDirect(System.err);

            tempOutputStream.close();
            tempErrorStream.close();
        }
    }
}
