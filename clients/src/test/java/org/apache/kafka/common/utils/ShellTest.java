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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(180)
@DisabledOnOs(OS.WINDOWS)
public class ShellTest {

    @Test
    public void testEchoHello() throws Exception {
        String output = Shell.execCommand("echo", "hello");
        assertEquals("hello\n", output);
    }

    @Test
    public void testHeadDevZero() throws Exception {
        final int length = 100000;
        String output = Shell.execCommand("head", "-c", Integer.toString(length), "/dev/zero");
        assertEquals(length, output.length());
    }

    private final static String NONEXISTENT_PATH = "/dev/a/path/that/does/not/exist/in/the/filesystem";

    @Test
    public void testAttemptToRunNonExistentProgram() {
        IOException e = assertThrows(IOException.class, () -> Shell.execCommand(NONEXISTENT_PATH),
                "Expected to get an exception when trying to run a program that does not exist");
        assertTrue(e.getMessage().contains("No such file"), "Unexpected error message '" + e.getMessage() + "'");
    }

    @Test
    public void testRunProgramWithErrorReturn() {
        Shell.ExitCodeException e = assertThrows(Shell.ExitCodeException.class,
            () -> Shell.execCommand("head", "-c", "0", NONEXISTENT_PATH));
        String message = e.getMessage();
        assertTrue(message.contains("No such file") || message.contains("illegal byte count"),
            "Unexpected error message '" + message + "'");
    }
}
