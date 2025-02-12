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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigTest;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigCommandTest {
    private static final List<String> BROKER_BOOTSTRAP = Arrays.asList("--bootstrap-server", "localhost:9092");
    private static final List<String> CONTROLLER_BOOTSTRAP = Arrays.asList("--bootstrap-controller", "localhost:9093");

    @Test
    public void shouldExitWithNonZeroStatusOnArgError() {
        assertNonZeroStatusExit("--blah");
    }

    @Test
    public void shouldExitWithNonZeroStatusAlterUserQuotaWithoutEntityName() {
        assertNonZeroStatusExit(toArray(BROKER_BOOTSTRAP, Arrays.asList(
            "--entity-type", "users",
            "--alter", "--add-config", "consumer_byte_rate=20000")));
    }

    @Test
    public void shouldExitWithNonZeroStatusOnBrokerCommandError() {
        assertNonZeroStatusExit("--bootstrap-server", "invalid host",
            "--entity-type", "brokers",
            "--entity-name", "1",
            "--describe");
    }

    @Test
    public void shouldExitWithNonZeroStatusIfBothBootstrapServerAndBootstrapControllerGiven() {
        assertNonZeroStatusExit(toArray(BROKER_BOOTSTRAP, CONTROLLER_BOOTSTRAP, Arrays.asList(
            "--describe", "--broker-defaults")));
    }

    public static void assertNonZeroStatusExit(String... args) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });

        try {
            ConfigCommand.main(args);
        } catch (RuntimeException e) {
            // do nothing.
        } finally {
            Exit.resetExitProcedure();
        }

        assertNotNull(exitStatus.get());
        assertEquals(1, exitStatus.get());
    }

    @Test
    public void shouldParseArgumentsForClientsEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "clients");
    }

    @Test
    public void shouldParseArgumentsForClientsEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "clients");
    }

    @Test
    public void shouldParseArgumentsForUsersEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "users");
    }

    @Test
    public void shouldParseArgumentsForUsersEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "users");
    }

    @Test
    public void shouldParseArgumentsForTopicsEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "topics");
    }

    @Test
    public void shouldParseArgumentsForTopicsEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "topics");
    }

    @Test
    public void shouldParseArgumentsForBrokersEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "brokers");
    }

    @Test
    public void shouldParseArgumentsForBrokersEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "brokers");
    }

    @Test
    public void shouldParseArgumentsForBrokerLoggersEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "broker-loggers");
    }

    @Test
    public void shouldParseArgumentsForBrokerLoggersEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "broker-loggers");
    }

    @Test
    public void shouldParseArgumentsForIpEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "ips");
    }

    @Test
    public void shouldParseArgumentsForIpEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "ips");
    }

    @Test
    public void shouldParseArgumentsForGroupEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "groups");
    }

    @Test
    public void shouldParseArgumentsForGroupEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "groups");
    }

    public void testArgumentParse(List<String> bootstrapArguments, String entityType) {
        String shortFlag = "--" + entityType.substring(0, entityType.length() - 1);
        String connectOpts1 = bootstrapArguments.get(0);
        String connectOpts2 = bootstrapArguments.get(1);

        // Should parse correctly
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            "--entity-name", "1",
            "--entity-type", entityType,
            "--describe"));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--describe"));
        createOpts.checkArgs();

        // For --alter and added config
        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            "--entity-name", "1",
            "--entity-type", entityType,
            "--alter",
            "--add-config", "a=b,c=d"));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            "--entity-name", "1",
            "--entity-type", entityType,
            "--alter",
            "--add-config-file", "/tmp/new.properties"));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--add-config", "a=b,c=d"));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--add-config-file", "/tmp/new.properties"));
        createOpts.checkArgs();

        // For alter and deleted config
        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            "--entity-name", "1",
            "--entity-type", entityType,
            "--alter",
            "--delete-config", "a,b,c"));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--delete-config", "a,b,c"));
        createOpts.checkArgs();

        // For alter and both added, deleted config
        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            "--entity-name", "1",
            "--entity-type", entityType,
            "--alter",
            "--add-config", "a=b,c=d",
            "--delete-config", "a"));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--add-config", "a=b,c=d",
            "--delete-config", "a"));
        createOpts.checkArgs();

        Properties addedProps = ConfigCommand.parseConfigsToBeAdded(createOpts);
        assertEquals(2, addedProps.size());
        assertEquals("b", addedProps.getProperty("a"));
        assertEquals("d", addedProps.getProperty("c"));

        Seq<String> deletedProps = ConfigCommand.parseConfigsToBeDeleted(createOpts);
        assertEquals(1, deletedProps.size());
        assertEquals("a", deletedProps.apply(0));

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            "--entity-name", "1",
            "--entity-type", entityType,
            "--alter",
            "--add-config", "a=b,c=,d=e,f="));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--add-config", "a._-c=b,c=,d=e,f="));
        createOpts.checkArgs();

        Properties addedProps2 = ConfigCommand.parseConfigsToBeAdded(createOpts);
        assertEquals(4, addedProps2.size());
        assertEquals("b", addedProps2.getProperty("a._-c"));
        assertEquals("e", addedProps2.getProperty("d"));
        assertTrue(addedProps2.getProperty("c").isEmpty());
        assertTrue(addedProps2.getProperty("f").isEmpty());

        ConfigCommand.ConfigCommandOptions inValidCreateOpts = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--add-config", "a;c=b"));

        assertThrows(IllegalArgumentException.class,
            () -> ConfigCommand.parseConfigsToBeAdded(inValidCreateOpts));

        ConfigCommand.ConfigCommandOptions inValidCreateOpts2 = new ConfigCommand.ConfigCommandOptions(toArray(connectOpts1, connectOpts2,
            shortFlag, "1",
            "--alter",
            "--add-config", "a,=b"));

        assertThrows(IllegalArgumentException.class,
            () -> ConfigCommand.parseConfigsToBeAdded(inValidCreateOpts2));
    }

    @Test
    public void shouldFailIfAddAndAddFile() {
        // Should not parse correctly
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", "1",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "a=b,c=d",
            "--add-config-file", "/tmp/new.properties"
        ));
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);
    }

    @Test
    public void testEntityDefaultForType() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "topics",
            "--describe"
        ));
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "clients",
            "--describe"
        ));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "users",
            "--describe"
        ));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "brokers",
            "--describe"
        ));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "broker-loggers",
            "--describe"
        ));
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "ips",
            "--describe"
        ));
        createOpts.checkArgs();

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "client-metrics",
            "--describe"
        ));
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);

        createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-default",
            "--entity-type", "groups",
            "--describe"
        ));
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);
    }

    @Test
    public void testParseConfigsToBeAddedForAddConfigFile() throws IOException {
        String fileContents =
            "a=b\n" +
            "c = d\n" +
            "json = {\"key\": \"val\"}\n" +
            "nested = [[1, 2], [3, 4]]";

        File file = TestUtils.tempFile(fileContents);

        List<String> addConfigFileArgs = Arrays.asList("--add-config-file", file.getPath());

        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList("--bootstrap-server", "localhost:9092",
                "--entity-name", "1",
                "--entity-type", "brokers",
                "--alter"),
            addConfigFileArgs));
        createOpts.checkArgs();

        Properties addedProps = ConfigCommand.parseConfigsToBeAdded(createOpts);
        assertEquals(4, addedProps.size());
        assertEquals("b", addedProps.getProperty("a"));
        assertEquals("d", addedProps.getProperty("c"));
        assertEquals("{\"key\": \"val\"}", addedProps.getProperty("json"));
        assertEquals("[[1, 2], [3, 4]]", addedProps.getProperty("nested"));
    }

    public void testExpectedEntityTypeNames(List<String> expectedTypes, List<String> expectedNames, List<String> connectOpts, String... args) {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList(connectOpts.get(0), connectOpts.get(1), "--describe"), Arrays.asList(args)));
        createOpts.checkArgs();
        assertEquals(createOpts.entityTypes().toSeq(), seq(expectedTypes));
        assertEquals(createOpts.entityNames().toSeq(), seq(expectedNames));
    }

    @Test
    public void testOptionEntityTypeNames() {
        List<String> connectOpts = Arrays.asList("--bootstrap-server", "localhost:9092");

        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.TOPIC), Collections.singletonList("A"), connectOpts, "--entity-type", "topics", "--entity-name", "A");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.IP), Collections.singletonList("1.2.3.4"), connectOpts, "--entity-name", "1.2.3.4", "--entity-type", "ips");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.CLIENT_METRICS), Collections.singletonList("A"), connectOpts, "--entity-type", "client-metrics", "--entity-name", "A");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.GROUP), Collections.singletonList("A"), connectOpts, "--entity-type", "groups", "--entity-name", "A");
        testExpectedEntityTypeNames(Arrays.asList(ConfigType.USER, ConfigType.CLIENT), Arrays.asList("A", ""), connectOpts,
            "--entity-type", "users", "--entity-type", "clients", "--entity-name", "A", "--entity-default");
        testExpectedEntityTypeNames(Arrays.asList(ConfigType.USER, ConfigType.CLIENT), Arrays.asList("", "B"), connectOpts,
            "--entity-default", "--entity-name", "B", "--entity-type", "users", "--entity-type", "clients");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.TOPIC), Collections.singletonList("A"), connectOpts, "--topic", "A");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.IP), Collections.singletonList("1.2.3.4"), connectOpts, "--ip", "1.2.3.4");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.GROUP), Collections.singletonList("A"), connectOpts, "--group", "A");
        testExpectedEntityTypeNames(Arrays.asList(ConfigType.CLIENT, ConfigType.USER), Arrays.asList("B", "A"), connectOpts, "--client", "B", "--user", "A");
        testExpectedEntityTypeNames(Arrays.asList(ConfigType.CLIENT, ConfigType.USER), Arrays.asList("B", ""), connectOpts, "--client", "B", "--user-defaults");
        testExpectedEntityTypeNames(Arrays.asList(ConfigType.CLIENT, ConfigType.USER), Collections.singletonList("A"), connectOpts,
            "--entity-type", "clients", "--entity-type", "users", "--entity-name", "A");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.TOPIC), Collections.emptyList(), connectOpts, "--entity-type", "topics");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.IP), Collections.emptyList(), connectOpts, "--entity-type", "ips");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.GROUP), Collections.emptyList(), connectOpts, "--entity-type", "groups");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.CLIENT_METRICS), Collections.emptyList(), connectOpts, "--entity-type", "client-metrics");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.BROKER), Collections.singletonList("0"), connectOpts, "--entity-name", "0", "--entity-type", "brokers");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.BROKER), Collections.singletonList("0"), connectOpts, "--broker", "0");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.USER), Collections.emptyList(), connectOpts, "--entity-type", "users");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.BROKER), Collections.emptyList(), connectOpts, "--entity-type", "brokers");
    }

    @Test
    public void shouldFailIfUnrecognisedEntityType() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"});
        assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts));
    }

    @Test
    public void shouldFailIfBrokerEntityTypeIsNotAnInteger() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-name", "A", "--entity-type", "brokers", "--alter", "--add-config", "a=b,c=d"});
        assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts));
    }

    @Test
    public void shouldFailIfShortBrokerEntityTypeIsNotAnInteger() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(new String[]{"--bootstrap-server", "localhost:9092",
            "--broker", "A", "--alter", "--add-config", "a=b,c=d"});
        assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts));
    }

    @Test
    public void shouldFailIfMixedEntityTypeFlags() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-name", "A", "--entity-type", "users", "--client", "B", "--describe"});
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);
    }

    @Test
    public void shouldFailIfInvalidHost() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-name", "A,B", "--entity-type", "ips", "--describe"});
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);
    }

    @Test
    public void shouldFailIfUnresolvableHost() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-name", "RFC2606.invalid", "--entity-type", "ips", "--describe"});
        assertThrows(IllegalArgumentException.class, createOpts::checkArgs);
    }

    private Entry<List<String>, Map<String, String>> argsAndExpectedEntity(Optional<String> entityName, String entityType) {
        String command;
        switch (entityType) {
            case ClientQuotaEntity.USER:
                command = "users";
                break;
            case ClientQuotaEntity.CLIENT_ID:
                command = "clients";
                break;
            case ClientQuotaEntity.IP:
                command = "ips";
                break;
            default:
                throw new IllegalArgumentException("Unknown command: " + entityType);
        }

        return entityName.map(name -> {
            if (name.isEmpty())
                return new SimpleImmutableEntry<>(Arrays.asList("--entity-type", command, "--entity-default"), Collections.singletonMap(entityType, (String) null));
            return new SimpleImmutableEntry<>(Arrays.asList("--entity-type", command, "--entity-name", name), Collections.singletonMap(entityType, name));
        }).orElse(new SimpleImmutableEntry<>(Collections.emptyList(), Collections.emptyMap()));
    }

    private void verifyAlterCommandFails(String expectedErrorMessage, List<String> alterOpts) {
        Admin mockAdminClient = mock(Admin.class);
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList("--bootstrap-server", "localhost:9092",
            "--alter"), alterOpts));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(mockAdminClient, opts));
        assertTrue(e.getMessage().contains(expectedErrorMessage), "Unexpected exception: " + e);
    }

    @Test
    public void shouldNotAlterNonQuotaIpConfigsUsingBootstrapServer() {
        // when using --bootstrap-server, it should be illegal to alter anything that is not a connection quota
        // for ip entities
        List<String> ipEntityOpts = Arrays.asList("--entity-type", "ips", "--entity-name", "127.0.0.1");
        String invalidProp = "some_config";
        verifyAlterCommandFails(invalidProp, concat(ipEntityOpts, Arrays.asList("--add-config", "connection_creation_rate=10000,some_config=10")));
        verifyAlterCommandFails(invalidProp, concat(ipEntityOpts, Arrays.asList("--add-config", "some_config=10")));
        verifyAlterCommandFails(invalidProp, concat(ipEntityOpts, Arrays.asList("--delete-config", "connection_creation_rate=10000,some_config=10")));
        verifyAlterCommandFails(invalidProp, concat(ipEntityOpts, Arrays.asList("--delete-config", "some_config=10")));
    }

    private void verifyDescribeQuotas(List<String> describeArgs, ClientQuotaFilter expectedFilter) {
        ConfigCommand.ConfigCommandOptions describeOpts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList("--bootstrap-server", "localhost:9092",
            "--describe"), describeArgs));
        KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> describeFuture = new KafkaFutureImpl<>();
        describeFuture.complete(Collections.emptyMap());
        DescribeClientQuotasResult describeResult = mock(DescribeClientQuotasResult.class);
        when(describeResult.entities()).thenReturn(describeFuture);

        AtomicBoolean describedConfigs = new AtomicBoolean();
        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
                assertTrue(filter.strict());
                assertEquals(new HashSet<>(expectedFilter.components()), new HashSet<>(filter.components()));
                describedConfigs.set(true);
                return describeResult;
            }
        };
        ConfigCommand.describeConfig(mockAdminClient, describeOpts);
        assertTrue(describedConfigs.get());
    }

    @Test
    public void testDescribeIpConfigs() {
        String entityType = ClientQuotaEntity.IP;
        String knownHost = "1.2.3.4";
        ClientQuotaFilter defaultIpFilter = ClientQuotaFilter.containsOnly(Collections.singletonList(ClientQuotaFilterComponent.ofDefaultEntity(entityType)));
        ClientQuotaFilter singleIpFilter = ClientQuotaFilter.containsOnly(Collections.singletonList(ClientQuotaFilterComponent.ofEntity(entityType, knownHost)));
        ClientQuotaFilter allIpsFilter = ClientQuotaFilter.containsOnly(Collections.singletonList(ClientQuotaFilterComponent.ofEntityType(entityType)));
        verifyDescribeQuotas(Arrays.asList("--entity-default", "--entity-type", "ips"), defaultIpFilter);
        verifyDescribeQuotas(Collections.singletonList("--ip-defaults"), defaultIpFilter);
        verifyDescribeQuotas(Arrays.asList("--entity-type", "ips", "--entity-name", knownHost), singleIpFilter);
        verifyDescribeQuotas(Arrays.asList("--ip", knownHost), singleIpFilter);
        verifyDescribeQuotas(Arrays.asList("--entity-type", "ips"), allIpsFilter);
    }

    public void verifyAlterQuotas(List<String> alterOpts, ClientQuotaEntity expectedAlterEntity,
                                  Map<String, Double> expectedProps, Set<ClientQuotaAlteration.Op> expectedAlterOps) {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList("--bootstrap-server", "localhost:9092",
            "--alter"), alterOpts));

        AtomicBoolean describedConfigs = new AtomicBoolean();
        KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> describeFuture = new KafkaFutureImpl<>();
        describeFuture.complete(Collections.singletonMap(expectedAlterEntity, expectedProps));
        DescribeClientQuotasResult describeResult = mock(DescribeClientQuotasResult.class);
        when(describeResult.entities()).thenReturn(describeFuture);

        Set<ClientQuotaFilterComponent> expectedFilterComponents = expectedAlterEntity.entries().entrySet().stream().map(e -> {
            String entityType = e.getKey();
            String entityName = e.getValue();
            return entityName == null
                ? ClientQuotaFilterComponent.ofDefaultEntity(e.getKey())
                : ClientQuotaFilterComponent.ofEntity(entityType, entityName);
        }).collect(Collectors.toSet());

        AtomicBoolean alteredConfigs = new AtomicBoolean();
        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.complete(null);
        AlterClientQuotasResult alterResult = mock(AlterClientQuotasResult.class);
        when(alterResult.all()).thenReturn(alterFuture);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
                assertTrue(filter.strict());
                assertEquals(expectedFilterComponents, new HashSet<>(filter.components()));
                describedConfigs.set(true);
                return describeResult;
            }

            @Override
            public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
                assertFalse(options.validateOnly());
                assertEquals(1, entries.size());
                ClientQuotaAlteration alteration = entries.iterator().next();
                assertEquals(expectedAlterEntity, alteration.entity());
                Collection<ClientQuotaAlteration.Op> ops = alteration.ops();
                assertEquals(expectedAlterOps, new HashSet<>(ops));
                alteredConfigs.set(true);
                return alterResult;
            }
        };
        ConfigCommand.alterConfig(mockAdminClient, createOpts);
        assertTrue(describedConfigs.get());
        assertTrue(alteredConfigs.get());
    }

    @Test
    public void testAlterIpConfig() {
        Entry<List<String>, Map<String, String>> singleIpArgsAndEntity = argsAndExpectedEntity(Optional.of("1.2.3.4"), ClientQuotaEntity.IP);
        Entry<List<String>, Map<String, String>> defaultIpArgsAndEntity = argsAndExpectedEntity(Optional.of(""), ClientQuotaEntity.IP);


        List<String> deleteArgs = Arrays.asList("--delete-config", "connection_creation_rate");
        Set<ClientQuotaAlteration.Op> deleteAlterationOps = new HashSet<>(Collections.singletonList(new ClientQuotaAlteration.Op("connection_creation_rate", null)));
        Map<String, Double> propsToDelete = Collections.singletonMap("connection_creation_rate", 50.0);

        List<String> addArgs = Arrays.asList("--add-config", "connection_creation_rate=100");
        Set<ClientQuotaAlteration.Op> addAlterationOps = new HashSet<>(Collections.singletonList(new ClientQuotaAlteration.Op("connection_creation_rate", 100.0)));

        verifyAlterQuotas(
            concat(singleIpArgsAndEntity.getKey(), deleteArgs),
            new ClientQuotaEntity(singleIpArgsAndEntity.getValue()),
            propsToDelete,
            deleteAlterationOps);
        verifyAlterQuotas(
            concat(singleIpArgsAndEntity.getKey(), addArgs),
            new ClientQuotaEntity(singleIpArgsAndEntity.getValue()),
            Collections.emptyMap(),
            addAlterationOps);
        verifyAlterQuotas(
            concat(defaultIpArgsAndEntity.getKey(), deleteArgs),
            new ClientQuotaEntity(defaultIpArgsAndEntity.getValue()),
            propsToDelete,
            deleteAlterationOps);
        verifyAlterQuotas(
            concat(defaultIpArgsAndEntity.getKey(), addArgs),
            new ClientQuotaEntity(defaultIpArgsAndEntity.getValue()),
            Collections.emptyMap(),
            addAlterationOps);
    }

    private void verifyAlterUserClientQuotas(String user, String client) {
        List<String> alterArgs = Arrays.asList("--add-config", "consumer_byte_rate=20000,producer_byte_rate=10000",
            "--delete-config", "request_percentage");
        Map<String, Double> propsToDelete = Collections.singletonMap("request_percentage", 50.0);

        Set<ClientQuotaAlteration.Op> alterationOps = new HashSet<>(Arrays.asList(
            new ClientQuotaAlteration.Op("consumer_byte_rate", 20000d),
            new ClientQuotaAlteration.Op("producer_byte_rate", 10000d),
            new ClientQuotaAlteration.Op("request_percentage", null)
        ));

        Entry<List<String>, Map<String, String>> userArgsAndEntity = argsAndExpectedEntity(Optional.ofNullable(user), ClientQuotaEntity.USER);
        Entry<List<String>, Map<String, String>> clientArgsAndEntry = argsAndExpectedEntity(Optional.ofNullable(client), ClientQuotaEntity.CLIENT_ID);

        verifyAlterQuotas(
            concat(alterArgs, userArgsAndEntity.getKey(), clientArgsAndEntry.getKey()),
            new ClientQuotaEntity(concat(userArgsAndEntity.getValue(), clientArgsAndEntry.getValue())),
            propsToDelete,
            alterationOps);
    }

    @Test
    public void shouldAddClientConfig() {
        verifyAlterUserClientQuotas("test-user-1", "test-client-1");
        verifyAlterUserClientQuotas("test-user-2", "");
        verifyAlterUserClientQuotas("test-user-3", null);
        verifyAlterUserClientQuotas("", "test-client-2");
        verifyAlterUserClientQuotas("", "");
        verifyAlterUserClientQuotas("", null);
        verifyAlterUserClientQuotas(null, "test-client-3");
        verifyAlterUserClientQuotas(null, "");
    }

    private final List<String> userEntityOpts = Arrays.asList("--entity-type", "users", "--entity-name", "admin");
    private final List<String> clientEntityOpts = Arrays.asList("--entity-type", "clients", "--entity-name", "admin");
    private final List<String> addScramOpts = Arrays.asList("--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]");
    private final List<String> deleteScramOpts = Arrays.asList("--delete-config", "SCRAM-SHA-256");

    @Test
    public void shouldNotAlterNonQuotaNonScramUserOrClientConfigUsingBootstrapServer() {
        // when using --bootstrap-server, it should be illegal to alter anything that is not a quota and not a SCRAM credential
        // for both user and client entities
        String invalidProp = "some_config";
        verifyAlterCommandFails(invalidProp, concat(userEntityOpts,
            Arrays.asList("-add-config", "consumer_byte_rate=20000,producer_byte_rate=10000,some_config=10")));
        verifyAlterCommandFails(invalidProp, concat(userEntityOpts,
            Arrays.asList("--add-config", "consumer_byte_rate=20000,producer_byte_rate=10000,some_config=10")));
        verifyAlterCommandFails(invalidProp, concat(clientEntityOpts, Arrays.asList("--add-config", "some_config=10")));
        verifyAlterCommandFails(invalidProp, concat(userEntityOpts, Arrays.asList("--delete-config", "consumer_byte_rate,some_config")));
        verifyAlterCommandFails(invalidProp, concat(userEntityOpts, Arrays.asList("--delete-config", "SCRAM-SHA-256,some_config")));
        verifyAlterCommandFails(invalidProp, concat(clientEntityOpts, Arrays.asList("--delete-config", "some_config")));
    }

    @Test
    public void shouldNotAlterScramClientConfigUsingBootstrapServer() {
        // when using --bootstrap-server, it should be illegal to alter SCRAM credentials for client entities
        verifyAlterCommandFails("SCRAM-SHA-256", concat(clientEntityOpts, addScramOpts));
        verifyAlterCommandFails("SCRAM-SHA-256", concat(clientEntityOpts, deleteScramOpts));
    }

    @Test
    public void shouldNotCreateUserScramCredentialConfigWithUnderMinimumIterationsUsingBootstrapServer() {
        // when using --bootstrap-server, it should be illegal to create a SCRAM credential for a user
        // with an iterations value less than the minimum
        verifyAlterCommandFails("SCRAM-SHA-256", concat(userEntityOpts, Arrays.asList("--add-config", "SCRAM-SHA-256=[iterations=100,password=foo-secret]")));
    }

    @Test
    public void shouldNotAlterUserScramCredentialAndClientQuotaConfigsSimultaneouslyUsingBootstrapServer() {
        // when using --bootstrap-server, it should be illegal to alter both SCRAM credentials and quotas for user entities
        String expectedErrorMessage = "SCRAM-SHA-256";
        List<String> secondUserEntityOpts = Arrays.asList("--entity-type", "users", "--entity-name", "admin1");
        List<String> addQuotaOpts = Arrays.asList("--add-config", "consumer_byte_rate=20000");
        List<String> deleteQuotaOpts = Arrays.asList("--delete-config", "consumer_byte_rate");

        verifyAlterCommandFails(expectedErrorMessage, concat(userEntityOpts, addScramOpts, userEntityOpts, deleteQuotaOpts));
        verifyAlterCommandFails(expectedErrorMessage, concat(userEntityOpts, addScramOpts, secondUserEntityOpts, deleteQuotaOpts));
        verifyAlterCommandFails(expectedErrorMessage, concat(userEntityOpts, deleteScramOpts, userEntityOpts, addQuotaOpts));
        verifyAlterCommandFails(expectedErrorMessage, concat(userEntityOpts, deleteScramOpts, secondUserEntityOpts, addQuotaOpts));

        // change order of quota/SCRAM commands, verify alter still fails
        verifyAlterCommandFails(expectedErrorMessage, concat(userEntityOpts, deleteQuotaOpts, userEntityOpts, addScramOpts));
        verifyAlterCommandFails(expectedErrorMessage, concat(secondUserEntityOpts, deleteQuotaOpts, userEntityOpts, addScramOpts));
        verifyAlterCommandFails(expectedErrorMessage, concat(userEntityOpts, addQuotaOpts, userEntityOpts, deleteScramOpts));
        verifyAlterCommandFails(expectedErrorMessage, concat(secondUserEntityOpts, addQuotaOpts, userEntityOpts, deleteScramOpts));
    }

    public void verifyUserScramCredentialsNotDescribed(List<String> requestOpts) {
        // User SCRAM credentials should not be described when specifying
        // --describe --entity-type users --entity-default (or --user-defaults) with --bootstrap-server
        KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> describeFuture = new KafkaFutureImpl<>();
        describeFuture.complete(Collections.singletonMap(new ClientQuotaEntity(Collections.singletonMap("", "")), Collections.singletonMap("request_percentage", 50.0)));
        DescribeClientQuotasResult describeClientQuotasResult = mock(DescribeClientQuotasResult.class);
        when(describeClientQuotasResult.entities()).thenReturn(describeFuture);
        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
                return describeClientQuotasResult;
            }

            @Override
            public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
                throw new IllegalStateException("Incorrectly described SCRAM credentials when specifying --entity-default with --bootstrap-server");
            }
        };
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList("--bootstrap-server", "localhost:9092", "--describe"), requestOpts));
        ConfigCommand.describeConfig(mockAdminClient, opts); // fails if describeUserScramCredentials() is invoked
    }

    @Test
    public void shouldNotDescribeUserScramCredentialsWithEntityDefaultUsingBootstrapServer() {
        String expectedMsg = "The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.";
        List<String> defaultUserOpt = Collections.singletonList("--user-defaults");
        List<String> verboseDefaultUserOpts = Arrays.asList("--entity-type", "users", "--entity-default");
        verifyAlterCommandFails(expectedMsg, concat(verboseDefaultUserOpts, addScramOpts));
        verifyAlterCommandFails(expectedMsg, concat(verboseDefaultUserOpts, deleteScramOpts));
        verifyUserScramCredentialsNotDescribed(verboseDefaultUserOpts);
        verifyAlterCommandFails(expectedMsg, concat(defaultUserOpt, addScramOpts));
        verifyAlterCommandFails(expectedMsg, concat(defaultUserOpt, deleteScramOpts));
        verifyUserScramCredentialsNotDescribed(defaultUserOpt);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldAlterTopicConfig(boolean file) {
        String filePath = "";
        Map<String, String> addedConfigs = new HashMap<>();
        addedConfigs.put("delete.retention.ms", "1000000");
        addedConfigs.put("min.insync.replicas", "2");
        if (file) {
            File f = kafka.utils.TestUtils.tempPropertiesFile(CollectionConverters.asScala(addedConfigs));
            filePath = f.getPath();
        }

        String resourceName = "my-topic";
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", resourceName,
            "--entity-type", "topics",
            "--alter",
            file ? "--add-config-file" : "--add-config",
            file ? filePath : addedConfigs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")),
            "--delete-config", "unclean.leader.election.enable"));
        AtomicBoolean alteredConfigs = new AtomicBoolean();

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName);
        List<ConfigEntry> configEntries = Arrays.asList(newConfigEntry("min.insync.replicas", "1"), newConfigEntry("unclean.leader.election.enable", "1"));
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(configEntries)));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.complete(null);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.all()).thenReturn(alterFuture);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertFalse(options.includeSynonyms(), "Config synonyms requested unnecessarily");
                assertEquals(1, resources.size());
                ConfigResource res = resources.iterator().next();
                assertEquals(res.type(), ConfigResource.Type.TOPIC);
                assertEquals(res.name(), resourceName);
                return describeResult;
            }

            @Override
            public synchronized AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
                assertEquals(1, configs.size());
                Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry = configs.entrySet().iterator().next();
                Collection<AlterConfigOp> alterConfigOps = entry.getValue();
                assertEquals(ConfigResource.Type.TOPIC, entry.getKey().type());
                assertEquals(3, alterConfigOps.size());

                Set<AlterConfigOp> expectedConfigOps = new HashSet<>(Arrays.asList(
                    new AlterConfigOp(newConfigEntry("delete.retention.ms", "1000000"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(newConfigEntry("min.insync.replicas", "2"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(newConfigEntry("unclean.leader.election.enable", ""), AlterConfigOp.OpType.DELETE)
                ));
                assertEquals(expectedConfigOps.size(), alterConfigOps.size());
                expectedConfigOps.forEach(expectedOp -> {
                    Optional<AlterConfigOp> actual = alterConfigOps.stream()
                        .filter(op -> Objects.equals(op.configEntry().name(), expectedOp.configEntry().name()))
                        .findFirst();
                    assertTrue(actual.isPresent());
                    assertEquals(expectedOp.opType(), actual.get().opType());
                    assertEquals(expectedOp.configEntry().name(), actual.get().configEntry().name());
                    assertEquals(expectedOp.configEntry().value(), actual.get().configEntry().value());
                });
                alteredConfigs.set(true);
                return alterResult;
            }
        };
        ConfigCommand.alterConfig(mockAdminClient, alterOpts);
        assertTrue(alteredConfigs.get());
        verify(describeResult).all();
    }

    public ConfigEntry newConfigEntry(String name, String value) {
        return ConfigTest.newConfigEntry(name, value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList());
    }

    @Test
    public void shouldDescribeConfigSynonyms() {
        String resourceName = "my-topic";
        ConfigCommand.ConfigCommandOptions describeOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", resourceName,
            "--entity-type", "topics",
            "--describe",
            "--all"));

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName);
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(Collections.emptyList())));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertTrue(options.includeSynonyms(), "Synonyms not requested");
                assertEquals(Collections.singleton(resource), new HashSet<>(resources));
                return describeResult;
            }
        };
        ConfigCommand.describeConfig(mockAdminClient, describeOpts);
        verify(describeResult).all();
    }

    @Test
    public void shouldAddBrokerLoggerConfig() {
        Node node = new Node(1, "localhost", 9092);
        verifyAlterBrokerLoggerConfig(node, "1", "1", Arrays.asList(
            new ConfigEntry("kafka.log.LogCleaner", "INFO"),
            new ConfigEntry("kafka.server.ReplicaManager", "INFO"),
            new ConfigEntry("kafka.server.KafkaApi", "INFO")
        ));
    }

    @Test
    public void testNoSpecifiedEntityOptionWithDescribeBrokersInBootstrapServerIsAllowed() {
        String[] optsList = new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-type", ConfigType.BROKER,
            "--describe"
        };

        new ConfigCommand.ConfigCommandOptions(optsList).checkArgs();
    }

    @Test
    public void testDescribeAllBrokerConfig() {
        String[] optsList = new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-type", ConfigType.BROKER,
            "--entity-name", "1",
            "--describe",
            "--all"};

        new ConfigCommand.ConfigCommandOptions(optsList).checkArgs();
    }

    @Test
    public void testDescribeAllTopicConfig() {
        String[] optsList = new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-type", ConfigType.TOPIC,
            "--entity-name", "foo",
            "--describe",
            "--all"};

        new ConfigCommand.ConfigCommandOptions(optsList).checkArgs();
    }

    @Test
    public void testEntityDefaultOptionWithDescribeBrokerLoggerIsNotAllowed() {
        String[] optsList = new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-type", ConfigCommand.BrokerLoggerConfigType(),
            "--entity-default",
            "--describe"
        };

        assertThrows(IllegalArgumentException.class, () -> new ConfigCommand.ConfigCommandOptions(optsList).checkArgs());
    }

    @Test
    public void testEntityDefaultOptionWithAlterBrokerLoggerIsNotAllowed() {
        String[] optsList = new String[]{"--bootstrap-server", "localhost:9092",
            "--entity-type", ConfigCommand.BrokerLoggerConfigType(),
            "--entity-default",
            "--alter",
            "--add-config", "kafka.log.LogCleaner=DEBUG"
        };

        assertThrows(IllegalArgumentException.class, () -> new ConfigCommand.ConfigCommandOptions(optsList).checkArgs());
    }

    @Test
    public void shouldRaiseInvalidConfigurationExceptionWhenAddingInvalidBrokerLoggerConfig() {
        Node node = new Node(1, "localhost", 9092);
        // verifyAlterBrokerLoggerConfig tries to alter kafka.log.LogCleaner, kafka.server.ReplicaManager and kafka.server.KafkaApi
        // yet, we make it so DescribeConfigs returns only one logger, implying that kafka.server.ReplicaManager and kafka.log.LogCleaner are invalid
        assertThrows(InvalidConfigurationException.class, () -> verifyAlterBrokerLoggerConfig(node, "1", "1", Collections.singletonList(
            new ConfigEntry("kafka.server.KafkaApi", "INFO")
        )));
    }

    @Test
    public void shouldAddDefaultBrokerDynamicConfig() {
        Node node = new Node(1, "localhost", 9092);
        verifyAlterBrokerConfig(node, "", Collections.singletonList("--entity-default"));
    }

    @Test
    public void shouldAddBrokerDynamicConfig() {
        Node node = new Node(1, "localhost", 9092);
        verifyAlterBrokerConfig(node, "1", Arrays.asList("--entity-name", "1"));
    }

    public void verifyAlterBrokerConfig(Node node, String resourceName, List<String> resourceOpts) {
        String[] optsList = toArray(Arrays.asList("--bootstrap-server", "localhost:9092",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "message.max.bytes=10,leader.replication.throttled.rate=10"), resourceOpts);
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(optsList);
        Map<String, String> brokerConfigs = new HashMap<>();
        brokerConfigs.put("num.io.threads", "5");

        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, resourceName);
        List<ConfigEntry> configEntries = Collections.singletonList(new ConfigEntry("num.io.threads", "5"));
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(configEntries)));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.complete(null);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.all()).thenReturn(alterFuture);

        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertFalse(options.includeSynonyms(), "Config synonyms requested unnecessarily");
                assertEquals(1, resources.size());
                ConfigResource res = resources.iterator().next();
                assertEquals(ConfigResource.Type.BROKER, res.type());
                assertEquals(resourceName, res.name());
                return describeResult;
            }

            @Override
            public synchronized AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
                assertEquals(1, configs.size());
                Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry = configs.entrySet().iterator().next();
                ConfigResource res = entry.getKey();
                Collection<AlterConfigOp> config = entry.getValue();
                assertEquals(ConfigResource.Type.BROKER, res.type());
                config.forEach(e -> brokerConfigs.put(e.configEntry().name(), e.configEntry().value()));
                return alterResult;
            }
        };
        ConfigCommand.alterConfig(mockAdminClient, alterOpts);
        Map<String, String> expected = new HashMap<>();
        expected.put("message.max.bytes", "10");
        expected.put("num.io.threads", "5");
        expected.put("leader.replication.throttled.rate", "10");
        assertEquals(expected, brokerConfigs);
        verify(describeResult).all();
    }

    @Test
    public void shouldDescribeConfigBrokerWithoutEntityName() {
        ConfigCommand.ConfigCommandOptions describeOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-type", "brokers",
            "--describe"));

        String brokerDefaultEntityName = "";
        ConfigResource resourceCustom = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ConfigResource resourceDefault = new ConfigResource(ConfigResource.Type.BROKER, brokerDefaultEntityName);
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        Config emptyConfig = new Config(Collections.emptyList());
        Map<ConfigResource, Config> resultMap = new HashMap<>();
        resultMap.put(resourceCustom, emptyConfig);
        resultMap.put(resourceDefault, emptyConfig);
        future.complete(resultMap);
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        // make sure it will be called 2 times: (1) for broker "1" (2) for default broker ""
        when(describeResult.all()).thenReturn(future);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertTrue(options.includeSynonyms(), "Synonyms not requested");
                ConfigResource resource = resources.iterator().next();
                assertEquals(ConfigResource.Type.BROKER, resource.type());
                assertTrue(Objects.equals(resourceCustom.name(), resource.name()) || Objects.equals(resourceDefault.name(), resource.name()));
                assertEquals(1, resources.size());
                return describeResult;
            }
        };
        ConfigCommand.describeConfig(mockAdminClient, describeOpts);
        verify(describeResult, times(2)).all();
    }

    private void verifyAlterBrokerLoggerConfig(Node node, String resourceName, String entityName,
                                               List<ConfigEntry> describeConfigEntries) {
        String[] optsList = toArray("--bootstrap-server", "localhost:9092",
            "--entity-type", ConfigCommand.BrokerLoggerConfigType(),
            "--alter",
            "--entity-name", entityName,
            "--add-config", "kafka.log.LogCleaner=DEBUG",
            "--delete-config", "kafka.server.ReplicaManager,kafka.server.KafkaApi");
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(optsList);
        AtomicBoolean alteredConfigs = new AtomicBoolean();

        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, resourceName);
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(describeConfigEntries)));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.complete(null);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.all()).thenReturn(alterFuture);

        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertEquals(1, resources.size());
                ConfigResource res = resources.iterator().next();
                assertEquals(ConfigResource.Type.BROKER_LOGGER, res.type());
                assertEquals(resourceName, res.name());
                return describeResult;
            }

            @Override
            public synchronized AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
                assertEquals(1, configs.size());
                Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry = configs.entrySet().iterator().next();
                ConfigResource res = entry.getKey();
                Collection<AlterConfigOp> alterConfigOps = entry.getValue();
                assertEquals(ConfigResource.Type.BROKER_LOGGER, res.type());
                assertEquals(3, alterConfigOps.size());

                List<AlterConfigOp> expectedConfigOps = Arrays.asList(
                    new AlterConfigOp(new ConfigEntry("kafka.server.ReplicaManager", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry("kafka.server.KafkaApi", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", "DEBUG"), AlterConfigOp.OpType.SET)
                );
                assertEquals(expectedConfigOps.size(), alterConfigOps.size());
                Iterator<AlterConfigOp> alterConfigOpsIter = alterConfigOps.iterator();
                for (AlterConfigOp expectedConfigOp : expectedConfigOps) {
                    assertEquals(expectedConfigOp, alterConfigOpsIter.next());
                }
                alteredConfigs.set(true);
                return alterResult;
            }
        };
        ConfigCommand.alterConfig(mockAdminClient, alterOpts);
        assertTrue(alteredConfigs.get());
        verify(describeResult).all();
    }

    @Test
    public void shouldNotUpdateBrokerConfigIfMalformedEntityName() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", "1,2,3", //Don't support multiple brokers currently
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "leader.replication.throttled.rate=10"));
        assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts));
    }

    @Test
    public void shouldNotUpdateBrokerConfigIfMalformedConfig() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", "1",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "a=="));
        assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts));
    }

    @Test
    public void shouldNotUpdateBrokerConfigIfMalformedBracketConfig() {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", "1",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "a=[b,c,d=e"));
        assertThrows(IllegalArgumentException.class, () -> ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts));
    }

    @Test
    public void shouldNotUpdateConfigIfNonExistingConfigIsDeleted() {
        String resourceName = "my-topic";
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-name", resourceName,
            "--entity-type", "topics",
            "--alter",
            "--delete-config", "missing_config1, missing_config2"));

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName);
        List<ConfigEntry> configEntries = Collections.emptyList();
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(configEntries)));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertEquals(1, resources.size());
                ConfigResource res = resources.iterator().next();
                assertEquals(res.type(), ConfigResource.Type.TOPIC);
                assertEquals(res.name(), resourceName);
                return describeResult;
            }
        };

        assertThrows(InvalidConfigurationException.class, () -> ConfigCommand.alterConfig(mockAdminClient, createOpts));
        verify(describeResult).all();
    }

    @Test
    public void shouldAlterClientMetricsConfig() {
        Node node = new Node(1, "localhost", 9092);
        verifyAlterClientMetricsConfig(node, "1", Arrays.asList("--entity-type", "client-metrics", "--entity-name", "1"));

        // Test for the --client-metrics alias
        node = new Node(1, "localhost", 9092);
        verifyAlterClientMetricsConfig(node, "1", Arrays.asList("--client-metrics", "1"));
    }

    private void verifyAlterClientMetricsConfig(Node node, String resourceName, List<String> resourceOpts) {
        List<String> optsList = concat(Arrays.asList("--bootstrap-server", "localhost:9092",
            "--alter",
            "--delete-config", "interval.ms",
            "--add-config", "metrics=org.apache.kafka.consumer.," +
                "match=[client_software_name=kafka.python,client_software_version=1\\.2\\..*]"), resourceOpts);
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(toArray(optsList));

        ConfigResource resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, resourceName);
        List<ConfigEntry> configEntries = Collections.singletonList(new ConfigEntry("interval.ms", "1000",
            ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG, false, false, Collections.emptyList(),
            ConfigEntry.ConfigType.UNKNOWN, null));
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(configEntries)));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.complete(null);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.all()).thenReturn(alterFuture);

        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertFalse(options.includeSynonyms(), "Config synonyms requested unnecessarily");
                assertEquals(1, resources.size());
                ConfigResource res = resources.iterator().next();
                assertEquals(ConfigResource.Type.CLIENT_METRICS, res.type());
                assertEquals(resourceName, res.name());
                return describeResult;
            }

            @Override
            public synchronized AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
                assertEquals(1, configs.size());
                Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry = configs.entrySet().iterator().next();
                ConfigResource res = entry.getKey();
                Collection<AlterConfigOp> alterConfigOps = entry.getValue();
                assertEquals(ConfigResource.Type.CLIENT_METRICS, res.type());
                assertEquals(3, alterConfigOps.size());

                List<AlterConfigOp> expectedConfigOps = Arrays.asList(
                    new AlterConfigOp(new ConfigEntry("interval.ms", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry("match", "client_software_name=kafka.python,client_software_version=1\\.2\\..*"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry("metrics", "org.apache.kafka.consumer."), AlterConfigOp.OpType.SET)
                );
                assertEquals(expectedConfigOps.size(), alterConfigOps.size());
                Iterator<AlterConfigOp> alterConfigOpsIter = alterConfigOps.iterator();
                for (AlterConfigOp expectedConfigOp : expectedConfigOps) {
                    assertEquals(expectedConfigOp, alterConfigOpsIter.next());
                }
                return alterResult;
            }
        };
        ConfigCommand.alterConfig(mockAdminClient, alterOpts);
        verify(describeResult).all();
        verify(alterResult).all();
    }

    @Test
    public void shouldDescribeClientMetricsConfigWithoutEntityName() {
        ConfigCommand.ConfigCommandOptions describeOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-type", "client-metrics",
            "--describe"));

        ConfigResource resourceCustom = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "1");
        ConfigEntry configEntry = new ConfigEntry("metrics", "*");
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertTrue(options.includeSynonyms());
                assertEquals(1, resources.size());
                ConfigResource resource = resources.iterator().next();
                assertEquals(ConfigResource.Type.CLIENT_METRICS, resource.type());
                assertEquals(resourceCustom.name(), resource.name());
                future.complete(Collections.singletonMap(resourceCustom, new Config(Collections.singletonList(configEntry))));
                return describeResult;
            }
        };
        mockAdminClient.incrementalAlterConfigs(Collections.singletonMap(resourceCustom,
            Collections.singletonList(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET))), new AlterConfigsOptions());
        ConfigCommand.describeConfig(mockAdminClient, describeOpts);
        verify(describeResult).all();
    }

    @Test
    public void shouldNotAlterClientMetricsConfigWithoutEntityName() {
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-type", "client-metrics",
            "--alter",
            "--add-config", "interval.ms=1000"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, alterOpts::checkArgs);
        assertEquals("An entity name must be specified with --alter of client-metrics", exception.getMessage());
    }

    @Test
    public void shouldAlterGroupConfig() {
        Node node = new Node(1, "localhost", 9092);
        verifyAlterGroupConfig(node, "group", Arrays.asList("--entity-type", "groups", "--entity-name", "group"));

        // Test for the --group alias
        verifyAlterGroupConfig(node, "groupUsingAlias", Arrays.asList("--group", "groupUsingAlias"));
    }

    private void verifyAlterGroupConfig(Node node, String resourceName, List<String> resourceOpts) {
        List<String> optsList = concat(Arrays.asList("--bootstrap-server", "localhost:9092",
            "--alter",
            "--delete-config", "consumer.session.timeout.ms",
            "--add-config", "consumer.heartbeat.interval.ms=6000"), resourceOpts);
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(toArray(optsList));

        ConfigResource resource = new ConfigResource(ConfigResource.Type.GROUP, resourceName);
        List<ConfigEntry> configEntries = Collections.singletonList(new ConfigEntry("consumer.session.timeout.ms", "45000",
            ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG, false, false, Collections.emptyList(),
            ConfigEntry.ConfigType.UNKNOWN, null));
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(resource, new Config(configEntries)));
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.complete(null);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.all()).thenReturn(alterFuture);

        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertFalse(options.includeSynonyms(), "Config synonyms requested unnecessarily");
                assertEquals(1, resources.size());
                ConfigResource res = resources.iterator().next();
                assertEquals(ConfigResource.Type.GROUP, res.type());
                assertEquals(resourceName, res.name());
                return describeResult;
            }

            @Override
            public synchronized AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
                assertEquals(1, configs.size());
                Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry = configs.entrySet().iterator().next();
                ConfigResource res = entry.getKey();
                Collection<AlterConfigOp> alterConfigOps = entry.getValue();
                assertEquals(ConfigResource.Type.GROUP, res.type());
                assertEquals(2, alterConfigOps.size());

                List<AlterConfigOp> expectedConfigOps = Arrays.asList(
                    new AlterConfigOp(new ConfigEntry("consumer.session.timeout.ms", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry("consumer.heartbeat.interval.ms", "6000"), AlterConfigOp.OpType.SET)
                );
                assertEquals(expectedConfigOps.size(), alterConfigOps.size());
                Iterator<AlterConfigOp> alterConfigOpsIter = alterConfigOps.iterator();
                for (AlterConfigOp expectedConfigOp : expectedConfigOps) {
                    assertEquals(expectedConfigOp, alterConfigOpsIter.next());
                }
                return alterResult;
            }
        };
        ConfigCommand.alterConfig(mockAdminClient, alterOpts);
        verify(describeResult).all();
        verify(alterResult).all();
    }

    @Test
    public void shouldDescribeGroupConfigWithoutEntityName() {
        ConfigCommand.ConfigCommandOptions describeOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-type", "groups",
            "--describe"));

        verifyDescribeGroupConfig(describeOpts, "group");
    }

    private void verifyDescribeGroupConfig(ConfigCommand.ConfigCommandOptions describeOpts, String resourceName) {
        ConfigResource resourceCustom = new ConfigResource(ConfigResource.Type.GROUP, resourceName);
        ConfigEntry configEntry = new ConfigEntry("consumer.heartbeat.interval.ms", "6000");
        KafkaFutureImpl<Map<ConfigResource, Config>> future = new KafkaFutureImpl<>();
        DescribeConfigsResult describeResult = mock(DescribeConfigsResult.class);
        when(describeResult.all()).thenReturn(future);

        Node node = new Node(1, "localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(node), node) {
            @Override
            public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
                assertTrue(options.includeSynonyms());
                assertEquals(1, resources.size());
                ConfigResource resource = resources.iterator().next();
                assertEquals(ConfigResource.Type.GROUP, resource.type());
                assertEquals(resourceCustom.name(), resource.name());
                future.complete(Collections.singletonMap(resourceCustom, new Config(Collections.singletonList(configEntry))));
                return describeResult;
            }
        };
        mockAdminClient.incrementalAlterConfigs(Collections.singletonMap(resourceCustom,
            Collections.singletonList(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET))), new AlterConfigsOptions());
        ConfigCommand.describeConfig(mockAdminClient, describeOpts);
        verify(describeResult).all();
    }

    @Test
    public void shouldNotAlterGroupConfigWithoutEntityName() {
        ConfigCommand.ConfigCommandOptions alterOpts = new ConfigCommand.ConfigCommandOptions(toArray("--bootstrap-server", "localhost:9092",
            "--entity-type", "groups",
            "--alter",
            "--add-config", "consumer.heartbeat.interval.ms=6000"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, alterOpts::checkArgs);
        assertEquals("An entity name must be specified with --alter of groups", exception.getMessage());
    }

    public static String[] toArray(String... first) {
        return first;
    }

    @SafeVarargs
    public static String[] toArray(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toArray(String[]::new);
    }

    @SafeVarargs
    public static List<String> concat(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toList();
    }

    @SafeVarargs
    public static <K, V> Map<K, V> concat(Map<K, V>... maps) {
        Map<K, V> res = new HashMap<>();
        Stream.of(maps)
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .forEach(e -> res.put(e.getKey(), e.getValue()));
        return res;
    }


    static class DummyAdminClient extends MockAdminClient {
        public DummyAdminClient(Node node) {
            super(Collections.singletonList(node), node);
        }

        @Override
        public synchronized DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
            return mock(DescribeConfigsResult.class);
        }

        @Override
        public synchronized AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
            return mock(AlterConfigsResult.class);
        }

        @Override
        public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
            return mock(DescribeClientQuotasResult.class);
        }

        @Override
        public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
            return mock(AlterClientQuotasResult.class);
        }
    }

    private <T> Seq<T> seq(Collection<T> seq) {
        return CollectionConverters.asScala(seq).toSeq();
    }
}
