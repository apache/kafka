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

import org.apache.kafka.shell.command.LsCommandHandler.ColumnSchema;
import org.apache.kafka.shell.command.LsCommandHandler.TargetDirectory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 120)
public class LsCommandHandlerTest {
    @Test
    public void testCalculateColumnSchema() {
        assertEquals(new ColumnSchema(1, 3),
            LsCommandHandler.calculateColumnSchema(OptionalInt.empty(),
                List.of("abc", "def", "ghi")));
        assertEquals(new ColumnSchema(1, 2),
            LsCommandHandler.calculateColumnSchema(OptionalInt.of(0),
                List.of("abc", "def")));
        assertEquals(new ColumnSchema(3, 1).setColumnWidths(3, 8, 6),
            LsCommandHandler.calculateColumnSchema(OptionalInt.of(80),
                List.of("a", "abcdef", "beta")));
        assertEquals(new ColumnSchema(2, 3).setColumnWidths(10, 7),
            LsCommandHandler.calculateColumnSchema(OptionalInt.of(18),
                List.of("alphabet", "beta", "gamma", "theta", "zeta")));
    }

    @Test
    public void testPrintEntries() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                    stream, StandardCharsets.UTF_8))) {
                LsCommandHandler.printEntries(writer, "", OptionalInt.of(18),
                    List.of("alphabet", "beta", "gamma", "theta", "zeta"));
            }
            assertEquals(String.join(String.format("%n"), List.of(
                "alphabet  theta",
                "beta      zeta",
                "gamma")), stream.toString().trim());
        }
    }

    @Test
    public void testPrintTargets() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                    stream, StandardCharsets.UTF_8))) {
                LsCommandHandler.printTargets(writer, OptionalInt.of(18),
                    List.of("foo", "foobarbaz", "quux"), List.of(
                        new TargetDirectory("/some/dir",
                            List.of("supercalifragalistic")),
                        new TargetDirectory("/some/other/dir",
                            List.of("capability", "delegation", "elephant",
                                "fungible", "green"))));
            }
            assertEquals(String.join(String.format("%n"), List.of(
                "foo        quux",
                "foobarbaz  ",
                "",
                "/some/dir:",
                "supercalifragalistic",
                "",
                "/some/other/dir:",
                "capability",
                "delegation",
                "elephant",
                "fungible",
                "green")), stream.toString().trim());
        }
    }
}
