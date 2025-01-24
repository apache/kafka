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

import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("dontUseSystemExit")
@ExtendWith(value = ClusterTestExtensions.class)
public class UserScramCredentialsCommandTest {
    private static final String USER1 = "user1";
    private static final String USER2 = "user2";

    private final ClusterInstance cluster;

    public UserScramCredentialsCommandTest(ClusterInstance cluster) {
        this.cluster = cluster;
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

    private ConfigCommandResult runConfigCommandViaBroker(String... args) {
        AtomicReference<OptionalInt> exitStatus = new AtomicReference<>(OptionalInt.empty());
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(OptionalInt.of(status));
            throw new RuntimeException();
        });

        List<String> commandArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", cluster.bootstrapServers()));
        commandArgs.addAll(Arrays.asList(args));
        try {
            String output = ConfigCommandIntegrationTest.captureStandardStream(false, () -> {
                ConfigCommand.main(commandArgs.toArray(new String[0]));
            });
            return new ConfigCommandResult(output);
        } catch (Exception e) {
            return new ConfigCommandResult("", exitStatus.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @ClusterTest
    public void testUserScramCredentialsRequests() throws Exception {
        createAndAlterUser(USER1);
        // now do the same thing for user2
        createAndAlterUser(USER2);

        // describe both
        // we don't know the order that quota or scram users come out, so we have 2 possibilities for each, 4 total
        String quotaPossibilityAOut = quotaMessage(USER1) + "\n" + quotaMessage(USER2);
        String quotaPossibilityBOut = quotaMessage(USER2) + "\n" + quotaMessage(USER1);
        String scramPossibilityAOut = describeUserMessage(USER1) + "\n" + describeUserMessage(USER2);
        String scramPossibilityBOut = describeUserMessage(USER2) + "\n" + describeUserMessage(USER1);
        describeUsers(
            quotaPossibilityAOut + "\n" + scramPossibilityAOut,
            quotaPossibilityAOut + "\n" + scramPossibilityBOut,
            quotaPossibilityBOut + "\n" + scramPossibilityAOut,
            quotaPossibilityBOut + "\n" + scramPossibilityBOut);

        // now delete configs, in opposite order, for user1 and user2, and describe
        deleteConfig(USER1, "consumer_byte_rate");
        deleteConfig(USER2, "SCRAM-SHA-256");
        describeUsers(quotaMessage(USER2) + "\n" + describeUserMessage(USER1));

        // now delete the rest of the configs, for user1 and user2, and describe
        deleteConfig(USER1, "SCRAM-SHA-256");
        deleteConfig(USER2, "consumer_byte_rate");
        describeUsers("");
    }

    @ClusterTest
    public void testAlterWithEmptyPassword() {
        String user1 = "user1";
        ConfigCommandResult result = runConfigCommandViaBroker("--user", user1, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=]");
        assertTrue(result.exitStatus.isPresent(), "Expected System.exit() to be called with an empty password");
        assertEquals(1, result.exitStatus.getAsInt(), "Expected empty password to cause failure with exit status=1");
    }

    @ClusterTest
    public void testDescribeUnknownUser() {
        String unknownUser = "unknownUser";
        ConfigCommandResult result = runConfigCommandViaBroker("--user", unknownUser, "--describe");
        assertFalse(result.exitStatus.isPresent(), "Expected System.exit() to not be called with an unknown user");
        assertEquals("", result.stdout);
    }

    private void createAndAlterUser(String user) throws InterruptedException {
        // create and describe a credential
        ConfigCommandResult result = runConfigCommandViaBroker("--user", user, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=foo-secret]");
        assertEquals(updateUserMessage(user), result.stdout);
        TestUtils.waitForCondition(
            () -> {
                try {
                    return Objects.equals(runConfigCommandViaBroker("--user", user, "--describe").stdout, describeUserMessage(user));
                } catch (Exception e) {
                    throw new NoRetryException(e);
                }
            },
            () -> "Failed to describe SCRAM credential change '" + user + "'");
        // create a user quota and describe the user again
        result = runConfigCommandViaBroker("--user", user, "--alter", "--add-config", "consumer_byte_rate=20000");
        assertEquals(updateUserMessage(user), result.stdout);
        TestUtils.waitForCondition(
            () -> {
                try {
                    return Objects.equals(runConfigCommandViaBroker("--user", user, "--describe").stdout, quotaMessage(user) + "\n" + describeUserMessage(user));
                } catch (Exception e) {
                    throw new NoRetryException(e);
                }
            },
            () -> "Failed to describe Quota change for '" + user + "'");
    }

    private void deleteConfig(String user, String config) {
        ConfigCommandResult result = runConfigCommandViaBroker("--user", user, "--alter", "--delete-config", config);
        assertEquals(updateUserMessage(user), result.stdout);
    }

    private void describeUsers(String... msgs) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> {
                try {
                    String output = runConfigCommandViaBroker("--entity-type", "users", "--describe").stdout;
                    return Arrays.asList(msgs).contains(output);
                } catch (Exception e) {
                    throw new NoRetryException(e);
                }
            },
            () -> "Failed to describe config");
    }

    private static String describeUserMessage(String user) {
        return "SCRAM credential configs for user-principal '" + user + "' are SCRAM-SHA-256=iterations=4096";
    }

    private static String updateUserMessage(String user) {
        return "Completed updating config for user " + user + ".";
    }

    private static String quotaMessage(String user) {
        return "Quota configs for user-principal '" + user + "' are consumer_byte_rate=20000.0";
    }
}
