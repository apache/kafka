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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 120)
public class CommandTest {
    @Test
    public void testParseCommands() {
        assertEquals(new CatCommandHandler(List.of("foo")),
            new Commands(true).parseCommand(List.of("cat", "foo")));
        assertEquals(new CdCommandHandler(Optional.empty()),
            new Commands(true).parseCommand(List.of("cd")));
        assertEquals(new CdCommandHandler(Optional.of("foo")),
            new Commands(true).parseCommand(List.of("cd", "foo")));
        assertEquals(new ExitCommandHandler(),
            new Commands(true).parseCommand(List.of("exit")));
        assertEquals(new HelpCommandHandler(),
            new Commands(true).parseCommand(List.of("help")));
        assertEquals(new HistoryCommandHandler(3),
            new Commands(true).parseCommand(List.of("history", "3")));
        assertEquals(new HistoryCommandHandler(Integer.MAX_VALUE),
            new Commands(true).parseCommand(List.of("history")));
        assertEquals(new LsCommandHandler(List.of()),
            new Commands(true).parseCommand(List.of("ls")));
        assertEquals(new LsCommandHandler(List.of("abc", "123")),
            new Commands(true).parseCommand(List.of("ls", "abc", "123")));
        assertEquals(new PwdCommandHandler(),
            new Commands(true).parseCommand(List.of("pwd")));
    }

    @Test
    public void testParseInvalidCommand() {
        assertEquals(new ErroneousCommandHandler("invalid choice: 'blah' (choose " +
            "from 'cat', 'cd', 'exit', 'find', 'help', 'history', 'ls', 'man', 'pwd', 'tree')"),
            new Commands(true).parseCommand(List.of("blah")));
    }

    @Test
    public void testEmptyCommandLine() {
        assertEquals(new NoOpCommandHandler(),
            new Commands(true).parseCommand(List.of("")));
        assertEquals(new NoOpCommandHandler(),
            new Commands(true).parseCommand(List.of()));
    }
}
