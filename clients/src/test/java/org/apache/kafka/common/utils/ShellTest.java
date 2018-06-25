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
package org.apache.kafka.common.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class ShellTest {
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(180);

    @Test
    public void testEchoHello() throws Exception {
        assumeTrue(!OperatingSystem.IS_WINDOWS);
        String output = Shell.execCommand("echo", "hello");
        assertEquals("hello\n", output);
    }

    @Test
    public void testHeadDevZero() throws Exception {
        assumeTrue(!OperatingSystem.IS_WINDOWS);
        final int length = 100000;
        String output = Shell.execCommand("head", "-c", Integer.toString(length), "/dev/zero");
        assertEquals(length, output.length());
    }

    private final static String NONEXISTENT_PATH = "/dev/a/path/that/does/not/exist/in/the/filesystem";

    @Test
    public void testAttemptToRunNonExistentProgram() throws Exception {
        assumeTrue(!OperatingSystem.IS_WINDOWS);
        try {
            Shell.execCommand(NONEXISTENT_PATH);
            fail("Expected to get an exception when trying to run a program that does not exist");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("No such file"));
        }
    }

    @Test
    public void testRunProgramWithErrorReturn() throws Exception {
        assumeTrue(!OperatingSystem.IS_WINDOWS);
        try {
            Shell.execCommand("head", "-c", "0", NONEXISTENT_PATH);
            fail("Expected to get an exception when trying to head a nonexistent file");
        } catch (Shell.ExitCodeException e) {
            String message = e.getMessage();
            assertTrue("Unexpected error message '" + message + "'",
                    message.contains("No such file") || message.contains("illegal byte count"));
        }
    }
}
