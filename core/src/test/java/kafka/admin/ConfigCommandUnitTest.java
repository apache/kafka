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

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigCommandUnitTest {
    private static final String ZK_CONNECT = "localhost:2181";

    private static final List<String> ZOOKEEPER_BOOTSTRAP = Arrays.asList("--zookeeper", ZK_CONNECT);
    private static final List<String> BROKER_BOOTSTRAP = Arrays.asList("--bootstrap-server", "localhost:9092");
    private static final List<String> CONTROLLER_BOOTSTRAP = Arrays.asList("--bootstrap-controller", "localhost:9093");

    @Test
    public void shouldExitWithNonZeroStatusOnArgError() {
        assertNonZeroStatusExit("--blah");
    }

    @Test
    public void shouldExitWithNonZeroStatusOnZkCommandWithTopicsEntity() {
        assertNonZeroStatusExit(toArray(ZOOKEEPER_BOOTSTRAP, Arrays.asList(
            "--entity-type", "topics",
            "--describe")));
    }

    @Test
    public void shouldExitWithNonZeroStatusOnZkCommandWithClientsEntity() {
        assertNonZeroStatusExit(toArray(ZOOKEEPER_BOOTSTRAP, Arrays.asList(
            "--entity-type", "clients",
            "--describe")));
    }

    @Test
    public void shouldExitWithNonZeroStatusOnZkCommandWithIpsEntity() {
        assertNonZeroStatusExit(toArray(ZOOKEEPER_BOOTSTRAP, Arrays.asList(
            "--entity-type", "ips",
            "--describe")));
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

    @Test
    public void shouldExitWithNonZeroStatusOnBrokerCommandWithZkTlsConfigFile() {
        assertNonZeroStatusExit(
            "--bootstrap-server", "invalid host",
            "--entity-type", "users",
            "--zk-tls-config-file", "zk_tls_config.properties",
            "--describe");
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
    public void shouldFailParseArgumentsForClientsEntityTypeUsingZookeeper() {
        assertThrows(IllegalArgumentException.class, () -> testArgumentParse(ZOOKEEPER_BOOTSTRAP, "clients"));
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
    public void shouldParseArgumentsForUsersEntityTypeUsingZookeeper() {
        testArgumentParse(ZOOKEEPER_BOOTSTRAP, "users");
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
    public void shouldFailParseArgumentsForTopicsEntityTypeUsingZookeeper() {
        assertThrows(IllegalArgumentException.class, () -> testArgumentParse(ZOOKEEPER_BOOTSTRAP, "topics"));
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
    public void shouldParseArgumentsForBrokersEntityTypeUsingZookeeper() {
        testArgumentParse(ZOOKEEPER_BOOTSTRAP, "brokers");
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
    public void shouldFailParseArgumentsForIpEntityTypeUsingZookeeper() {
        assertThrows(IllegalArgumentException.class, () -> testArgumentParse(ZOOKEEPER_BOOTSTRAP, "ips"));
    }

    @Test
    public void shouldParseArgumentsForIpEntityTypeWithBrokerBootstrap() {
        testArgumentParse(BROKER_BOOTSTRAP, "ips");
    }

    @Test
    public void shouldParseArgumentsForIpEntityTypeWithControllerBootstrap() {
        testArgumentParse(CONTROLLER_BOOTSTRAP, "ips");
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

    @SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
    public void testExpectedEntityTypeNames(List<String> expectedTypes, List<String> expectedNames, List<String> connectOpts, String...args) {
        ConfigCommand.ConfigCommandOptions createOpts = new ConfigCommand.ConfigCommandOptions(toArray(Arrays.asList(connectOpts.get(0), connectOpts.get(1), "--describe"), Arrays.asList(args)));
        createOpts.checkArgs();
        assertEquals(createOpts.entityTypes().toSeq(), ConfigCommandIntegrationTest.seq(expectedTypes));
        assertEquals(createOpts.entityNames().toSeq(), ConfigCommandIntegrationTest.seq(expectedNames));
    }

    public void doTestOptionEntityTypeNames(boolean zkConfig) {
        List<String> connectOpts = zkConfig
            ? Arrays.asList("--zookeeper", ZK_CONNECT)
            : Arrays.asList("--bootstrap-server", "localhost:9092");

        // zookeeper config only supports "users" and "brokers" entity type
        if (!zkConfig) {
            testExpectedEntityTypeNames(Collections.singletonList(ConfigType.TOPIC), Collections.singletonList("A"), connectOpts, "--entity-type", "topics", "--entity-name", "A");
            testExpectedEntityTypeNames(Collections.singletonList(ConfigType.IP), Collections.singletonList("1.2.3.4"), connectOpts, "--entity-name", "1.2.3.4", "--entity-type", "ips");
            testExpectedEntityTypeNames(Arrays.asList(ConfigType.USER, ConfigType.CLIENT), Arrays.asList("A", ""), connectOpts,
                "--entity-type", "users", "--entity-type", "clients", "--entity-name", "A", "--entity-default");
            testExpectedEntityTypeNames(Arrays.asList(ConfigType.USER, ConfigType.CLIENT), Arrays.asList("", "B"), connectOpts,
                "--entity-default", "--entity-name", "B", "--entity-type", "users", "--entity-type", "clients");
            testExpectedEntityTypeNames(Collections.singletonList(ConfigType.TOPIC), Collections.singletonList("A"), connectOpts, "--topic", "A");
            testExpectedEntityTypeNames(Collections.singletonList(ConfigType.IP), Collections.singletonList("1.2.3.4"), connectOpts, "--ip", "1.2.3.4");
            testExpectedEntityTypeNames(Arrays.asList(ConfigType.CLIENT, ConfigType.USER), Arrays.asList("B", "A"), connectOpts, "--client", "B", "--user", "A");
            testExpectedEntityTypeNames(Arrays.asList(ConfigType.CLIENT, ConfigType.USER), Arrays.asList("B", ""), connectOpts, "--client", "B", "--user-defaults");
            testExpectedEntityTypeNames(Arrays.asList(ConfigType.CLIENT, ConfigType.USER), Collections.singletonList("A"), connectOpts,
                "--entity-type", "clients", "--entity-type", "users", "--entity-name", "A");
            testExpectedEntityTypeNames(Collections.singletonList(ConfigType.TOPIC), Collections.emptyList(), connectOpts, "--entity-type", "topics");
            testExpectedEntityTypeNames(Collections.singletonList(ConfigType.IP), Collections.emptyList(), connectOpts, "--entity-type", "ips");
        }

        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.BROKER), Collections.singletonList("0"), connectOpts, "--entity-name", "0", "--entity-type", "brokers");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.BROKER), Collections.singletonList("0"), connectOpts, "--broker", "0");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.USER), Collections.emptyList(), connectOpts, "--entity-type", "users");
        testExpectedEntityTypeNames(Collections.singletonList(ConfigType.BROKER), Collections.emptyList(), connectOpts, "--entity-type", "brokers");
    }

    @Test
    public void testOptionEntityTypeNamesUsingZookeeper() {
        doTestOptionEntityTypeNames(true);
    }

    @Test
    public void testOptionEntityTypeNames() {
        doTestOptionEntityTypeNames(false);
    }

    public static String[] toArray(String... first) {
        return first;
    }

    @SafeVarargs
    public static String[] toArray(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toArray(String[]::new);
    }
}
