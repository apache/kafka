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
package org.apache.kafka.tools.config;

import kafka.server.BaseRequestTest;
import kafka.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.tools.config.ConfigCommandIntegrationTest.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.apache.kafka.tools.config.ConfigCommandTest.toArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UserScramCredentialsCommandTest extends BaseRequestTest {
    private static final Logger log = LoggerFactory.getLogger(UserScramCredentialsCommandTest.class);

    @Override
    public int brokerCount() {
        return 1;
    }

    static class ConfigCommandResult {
        public final String stdout;
        public final OptionalInt exitStatus;

        public ConfigCommandResult(String stdout) {
            this(stdout, OptionalInt.empty());
        }

        public ConfigCommandResult(String stdout, OptionalInt exitStatus) {
            this.stdout = stdout;
            this.exitStatus = exitStatus;
        }
    }

    private ConfigCommandResult runConfigCommandViaBroker(String...args) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        String utf8 = StandardCharsets.UTF_8.name();
        PrintStream printStream;
        try {
            printStream = new PrintStream(byteArrayOutputStream, true, utf8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        AtomicReference<OptionalInt> exitStatus = new AtomicReference<>(OptionalInt.empty());
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(OptionalInt.of((Integer) status));
            throw new RuntimeException();
        });
        String[] commandArgs = toArray(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName())), Arrays.asList(args));
        try {
            PrintStream prev = System.out;
            System.setOut(printStream);
            try {
                ConfigCommand.main(commandArgs);
            } finally {
                System.setOut(prev);
            }
            return new ConfigCommandResult(byteArrayOutputStream.toString(utf8));
        } catch (Exception e) {
            log.debug("Exception running ConfigCommand " + String.join(" ", commandArgs), e);
            return new ConfigCommandResult("", exitStatus.get());
        } finally {
            printStream.close();
            Exit.resetExitProcedure();
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft", "zk"})
    public void testUserScramCredentialsRequests(String quorum) throws InterruptedException {
        String user1 = "user1";
        // create and describe a credential
        ConfigCommandResult result = runConfigCommandViaBroker(toArray("--user", user1, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=foo-secret]"));
        String alterConfigsUser1Out = "Completed updating config for user " + user1 + ".\n";
        assertEquals(alterConfigsUser1Out, result.stdout);
        String scramCredentialConfigsUser1Out = "SCRAM credential configs for user-principal '" + user1 + "' are SCRAM-SHA-256=iterations=4096\n";
        TestUtils.waitForCondition(
            () -> Objects.equals(runConfigCommandViaBroker(toArray("--user", user1, "--describe")).stdout, scramCredentialConfigsUser1Out),
            () -> "Failed to describe SCRAM credential change '" + user1 + "'");
        // create a user quota and describe the user again
        result = runConfigCommandViaBroker(toArray("--user", user1, "--alter", "--add-config", "consumer_byte_rate=20000"));
        assertEquals(alterConfigsUser1Out, result.stdout);
        String quotaConfigsUser1Out = "Quota configs for user-principal '" + user1 + "' are consumer_byte_rate=20000.0\n";
        TestUtils.waitForCondition(
            () -> Objects.equals(runConfigCommandViaBroker(toArray("--user", user1, "--describe")).stdout, quotaConfigsUser1Out + scramCredentialConfigsUser1Out),
            () -> "Failed to describe Quota change for '" + user1 + "'");

        // now do the same thing for user2
        String user2 = "user2";
        // create and describe a credential
        result = runConfigCommandViaBroker(toArray("--user", user2, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=foo-secret]"));
        String alterConfigsUser2Out = "Completed updating config for user " + user2 + ".\n";
        assertEquals(alterConfigsUser2Out, result.stdout);
        String scramCredentialConfigsUser2Out = "SCRAM credential configs for user-principal '" + user2 + "' are SCRAM-SHA-256=iterations=4096\n";
        TestUtils.waitForCondition(
            () -> Objects.equals(runConfigCommandViaBroker(toArray("--user", user2, "--describe")).stdout, scramCredentialConfigsUser2Out),
            () -> "Failed to describe SCRAM credential change '" + user2 + "'");
        // create a user quota and describe the user again
        result = runConfigCommandViaBroker(toArray("--user", user2, "--alter", "--add-config", "consumer_byte_rate=20000"));
        assertEquals(alterConfigsUser2Out, result.stdout);
        String quotaConfigsUser2Out = "Quota configs for user-principal '" + user2 + "' are consumer_byte_rate=20000.0\n";
        TestUtils.waitForCondition(
            () -> Objects.equals(runConfigCommandViaBroker(toArray("--user", user2, "--describe")).stdout, quotaConfigsUser2Out + scramCredentialConfigsUser2Out),
            () -> "Failed to describe Quota change for '" + user2 + "'");

        // describe both
        result = runConfigCommandViaBroker(toArray("--entity-type", "users", "--describe"));
        // we don't know the order that quota or scram users come out, so we have 2 possibilities for each, 4 total
        String quotaPossibilityAOut = quotaConfigsUser1Out + quotaConfigsUser2Out;
        String quotaPossibilityBOut = quotaConfigsUser2Out + quotaConfigsUser1Out;
        String scramPossibilityAOut = scramCredentialConfigsUser1Out + scramCredentialConfigsUser2Out;
        String scramPossibilityBOut = scramCredentialConfigsUser2Out + scramCredentialConfigsUser1Out;
        assertTrue(result.stdout.equals(quotaPossibilityAOut + scramPossibilityAOut)
            || result.stdout.equals(quotaPossibilityAOut + scramPossibilityBOut)
            || result.stdout.equals(quotaPossibilityBOut + scramPossibilityAOut)
            || result.stdout.equals(quotaPossibilityBOut + scramPossibilityBOut));

        // now delete configs, in opposite order, for user1 and user2, and describe
        result = runConfigCommandViaBroker(toArray("--user", user1, "--alter", "--delete-config", "consumer_byte_rate"));
        assertEquals(alterConfigsUser1Out, result.stdout);
        result = runConfigCommandViaBroker(toArray("--user", user2, "--alter", "--delete-config", "SCRAM-SHA-256"));
        assertEquals(alterConfigsUser2Out, result.stdout);
        TestUtils.waitForCondition(
            () -> Objects.equals(runConfigCommandViaBroker(toArray("--entity-type", "users", "--describe")).stdout, quotaConfigsUser2Out + scramCredentialConfigsUser1Out),
            () -> "Failed to describe Quota change for '" + user2 + "'");

        // now delete the rest of the configs, for user1 and user2, and describe
        result = runConfigCommandViaBroker(toArray("--user", user1, "--alter", "--delete-config", "SCRAM-SHA-256"));
        assertEquals(alterConfigsUser1Out, result.stdout);
        result = runConfigCommandViaBroker(toArray("--user", user2, "--alter", "--delete-config", "consumer_byte_rate"));
        assertEquals(alterConfigsUser2Out, result.stdout);
        TestUtils.waitForCondition(() -> Objects.equals(runConfigCommandViaBroker(toArray("--entity-type", "users", "--describe")).stdout, ""),
            () -> "Failed to describe All users deleted");
    }

    @SuppressWarnings("dontUseSystemExit")
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft", "zk"})
    public void testAlterWithEmptyPassword(String quorum) {
        String user1 = "user1";
        ConfigCommandResult result = runConfigCommandViaBroker(toArray("--user", user1, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=]"));
        assertTrue(result.exitStatus.isPresent(), "Expected System.exit() to be called with an empty password");
        assertEquals(1, result.exitStatus.getAsInt(), "Expected empty password to cause failure with exit status=1");
    }

    @SuppressWarnings("dontUseSystemExit")
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft", "zk"})
    public void testDescribeUnknownUser(String quorum) {
        String unknownUser = "unknownUser";
        ConfigCommandResult result = runConfigCommandViaBroker(toArray("--user", unknownUser, "--describe"));
        assertFalse(result.exitStatus.isPresent(), "Expected System.exit() to not be called with an unknown user");
        assertEquals("", result.stdout);
    }
}
