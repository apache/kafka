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

package org.apache.kafka.shell.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

@Timeout(value = 120)
public class CommandTest {
    @Test
    public void testParseCommands() {
        assertEquals(new CatCommandHandler(Arrays.asList("foo")),
            new Commands(true).parseCommand(Arrays.asList("cat", "foo")));
        assertEquals(new CdCommandHandler(Optional.empty()),
            new Commands(true).parseCommand(Arrays.asList("cd")));
        assertEquals(new CdCommandHandler(Optional.of("foo")),
            new Commands(true).parseCommand(Arrays.asList("cd", "foo")));
        assertEquals(new ExitCommandHandler(),
            new Commands(true).parseCommand(Arrays.asList("exit")));
        assertEquals(new HelpCommandHandler(),
            new Commands(true).parseCommand(Arrays.asList("help")));
        assertEquals(new HistoryCommandHandler(3),
            new Commands(true).parseCommand(Arrays.asList("history", "3")));
        assertEquals(new HistoryCommandHandler(Integer.MAX_VALUE),
            new Commands(true).parseCommand(Arrays.asList("history")));
        assertEquals(new LsCommandHandler(Collections.emptyList()),
            new Commands(true).parseCommand(Arrays.asList("ls")));
        assertEquals(new LsCommandHandler(Arrays.asList("abc", "123")),
            new Commands(true).parseCommand(Arrays.asList("ls", "abc", "123")));
        assertEquals(new PwdCommandHandler(),
            new Commands(true).parseCommand(Arrays.asList("pwd")));
    }

    @Test
    public void testParseInvalidCommand() {
        assertEquals(new ErroneousCommandHandler("invalid choice: 'blah' (choose " +
            "from 'cat', 'cd', 'exit', 'find', 'help', 'history', 'ls', 'man', 'pwd', 'tree')"),
            new Commands(true).parseCommand(Arrays.asList("blah")));
    }

    @Test
    public void testEmptyCommandLine() {
        assertEquals(new NoOpCommandHandler(),
            new Commands(true).parseCommand(Arrays.asList("")));
        assertEquals(new NoOpCommandHandler(),
            new Commands(true).parseCommand(Collections.emptyList()));
    }
}
