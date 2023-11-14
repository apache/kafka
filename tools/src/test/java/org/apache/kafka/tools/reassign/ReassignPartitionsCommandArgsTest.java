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
package org.apache.kafka.tools.reassign;

import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
public class ReassignPartitionsCommandArgsTest {
    public static final String MISSING_BOOTSTRAP_SERVER_MSG = "Please specify --bootstrap-server";

    @BeforeEach
    public void setUp() {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new IllegalArgumentException(message);
        });
    }

    @AfterEach
    public void tearDown() {
        Exit.resetExitProcedure();
    }

    ///// Test valid argument parsing
    @Test
    public void shouldCorrectlyParseValidMinimumGenerateOptions() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--generate",
            "--broker-list", "101,102",
            "--topics-to-move-json-file", "myfile.json"};
        ReassignPartitionsCommand.validateAndParseArgs(args);
    }

    @Test
    public void shouldCorrectlyParseValidMinimumExecuteOptions() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute",
            "--reassignment-json-file", "myfile.json"};
        ReassignPartitionsCommand.validateAndParseArgs(args);
    }

    @Test
    public void shouldCorrectlyParseValidMinimumVerifyOptions() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--verify",
            "--reassignment-json-file", "myfile.json"};
        ReassignPartitionsCommand.validateAndParseArgs(args);
    }

    @Test
    public void shouldAllowThrottleOptionOnExecute() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute",
            "--throttle", "100",
            "--reassignment-json-file", "myfile.json"};
        ReassignPartitionsCommand.validateAndParseArgs(args);
    }

    @Test
    public void shouldUseDefaultsIfEnabled() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute",
            "--reassignment-json-file", "myfile.json"};
        ReassignPartitionsCommandOptions opts = ReassignPartitionsCommand.validateAndParseArgs(args);
        assertEquals(10000L, opts.options.valueOf(opts.timeoutOpt));
        assertEquals(-1L, opts.options.valueOf(opts.interBrokerThrottleOpt));
    }

    @Test
    public void testList() {
        String[] args = new String[] {
            "--list",
            "--bootstrap-server", "localhost:1234"};
        ReassignPartitionsCommand.validateAndParseArgs(args);
    }

    @Test
    public void testCancelWithPreserveThrottlesOption() {
        String[] args = new String[] {
            "--cancel",
            "--bootstrap-server", "localhost:1234",
            "--reassignment-json-file", "myfile.json",
            "--preserve-throttles"};
        ReassignPartitionsCommand.validateAndParseArgs(args);
    }

    ///// Test handling missing or invalid actions
    @Test
    public void shouldFailIfNoArgs() {
        String[] args = new String[0];
        shouldFailWith(ReassignPartitionsCommand.HELP_TEXT, args);
    }

    @Test
    public void shouldFailIfBlankArg() {
        String[] args = new String[] {" "};
        shouldFailWith("Command must include exactly one action", args);
    }

    @Test
    public void shouldFailIfMultipleActions() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute",
            "--verify",
            "--reassignment-json-file", "myfile.json"
        };
        shouldFailWith("Command must include exactly one action", args);
    }

    ///// Test --execute
    @Test
    public void shouldNotAllowExecuteWithTopicsOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute",
            "--reassignment-json-file", "myfile.json",
            "--topics-to-move-json-file", "myfile.json"};
        shouldFailWith("Option \"[topics-to-move-json-file]\" can't be used with action \"[execute]\"", args);
    }

    @Test
    public void shouldNotAllowExecuteWithBrokerList() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute",
            "--reassignment-json-file", "myfile.json",
            "--broker-list", "101,102"
        };
        shouldFailWith("Option \"[broker-list]\" can't be used with action \"[execute]\"", args);
    }

    @Test
    public void shouldNotAllowExecuteWithoutReassignmentOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--execute"};
        shouldFailWith("Missing required argument \"[reassignment-json-file]\"", args);
    }

    @Test
    public void testMissingBootstrapServerArgumentForExecute() {
        String[] args = new String[] {
            "--execute"};
        shouldFailWith(MISSING_BOOTSTRAP_SERVER_MSG, args);
    }

    ///// Test --generate
    @Test
    public void shouldNotAllowGenerateWithoutBrokersAndTopicsOptions() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--generate"};
        shouldFailWith("Missing required argument \"[topics-to-move-json-file]\"", args);
    }

    @Test
    public void shouldNotAllowGenerateWithoutBrokersOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--topics-to-move-json-file", "myfile.json",
            "--generate"};
        shouldFailWith("Missing required argument \"[broker-list]\"", args);
    }

    @Test
    public void shouldNotAllowGenerateWithoutTopicsOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--broker-list", "101,102",
            "--generate"};
        shouldFailWith("Missing required argument \"[topics-to-move-json-file]\"", args);
    }

    @Test
    public void shouldNotAllowGenerateWithThrottleOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--generate",
            "--broker-list", "101,102",
            "--throttle", "100",
            "--topics-to-move-json-file", "myfile.json"};
        shouldFailWith("Option \"[throttle]\" can't be used with action \"[generate]\"", args);
    }

    @Test
    public void shouldNotAllowGenerateWithReassignmentOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--generate",
            "--broker-list", "101,102",
            "--topics-to-move-json-file", "myfile.json",
            "--reassignment-json-file", "myfile.json"};
        shouldFailWith("Option \"[reassignment-json-file]\" can't be used with action \"[generate]\"", args);
    }

    @Test
    public void shouldPrintHelpTextIfHelpArg() {
        String[] args = new String[] {"--help"};
        // note, this is not actually a failed case, it's just we share the same `printUsageAndExit` method when wrong arg received
        shouldFailWith(ReassignPartitionsCommand.HELP_TEXT, args);
    }

    ///// Test --verify
    @Test
    public void shouldNotAllowVerifyWithoutReassignmentOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--verify"};
        shouldFailWith("Missing required argument \"[reassignment-json-file]\"", args);
    }

    @Test
    public void shouldNotAllowBrokersListWithVerifyOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--verify",
            "--broker-list", "100,101",
            "--reassignment-json-file", "myfile.json"};
        shouldFailWith("Option \"[broker-list]\" can't be used with action \"[verify]\"", args);
    }

    @Test
    public void shouldNotAllowThrottleWithVerifyOption() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--verify",
            "--throttle", "100",
            "--reassignment-json-file", "myfile.json"};
        shouldFailWith("Option \"[throttle]\" can't be used with action \"[verify]\"", args);
    }

    @Test
    public void shouldNotAllowTopicsOptionWithVerify() {
        String[] args = new String[] {
            "--bootstrap-server", "localhost:1234",
            "--verify",
            "--reassignment-json-file", "myfile.json",
            "--topics-to-move-json-file", "myfile.json"};
        shouldFailWith("Option \"[topics-to-move-json-file]\" can't be used with action \"[verify]\"", args);
    }

    private void shouldFailWith(String msg, String[] args) {
        Throwable e = assertThrows(Exception.class, () -> ReassignPartitionsCommand.validateAndParseArgs(args),
            () -> "Should have failed with [" + msg + "] but no failure occurred.");
        assertTrue(e.getMessage().startsWith(msg), "Expected exception with message:\n[" + msg + "]\nbut was\n[" + e.getMessage() + "]");
    }

    ///// Test --cancel
    @Test
    public void shouldNotAllowCancelWithoutBootstrapServerOption() {
        String[] args = new String[] {
            "--cancel"};
        shouldFailWith(MISSING_BOOTSTRAP_SERVER_MSG, args);
    }

    @Test
    public void shouldNotAllowCancelWithoutReassignmentJsonFile() {
        String[] args = new String[] {
            "--cancel",
            "--bootstrap-server", "localhost:1234",
            "--preserve-throttles"};
        shouldFailWith("Missing required argument \"[reassignment-json-file]\"", args);
    }
}
