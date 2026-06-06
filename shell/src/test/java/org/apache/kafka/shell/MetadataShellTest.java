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

package org.apache.kafka.shell;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120)
public class MetadataShellTest {

    @Test
    public void testParseCheckpoint() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        MetadataShell.MetadataShellArguments arguments = MetadataShell.parseArguments(
            new String[] {"--checkpoint", "metadata.checkpoint"},
            new PrintStream(stream, true, StandardCharsets.UTF_8));

        assertEquals("metadata.checkpoint", arguments.checkpointPath());
        assertEquals(List.of(), arguments.command());
        assertEquals("", stream.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testParseCheckpointWithCommand() throws Exception {
        MetadataShell.MetadataShellArguments arguments = MetadataShell.parseArguments(
            new String[] {"--checkpoint", "metadata.checkpoint", "ls", "/topics"},
            System.out);

        assertEquals("metadata.checkpoint", arguments.checkpointPath());
        assertEquals(List.of("ls", "/topics"), arguments.command());
    }

    @ParameterizedTest
    @ValueSource(strings = {"--snapshot", "-s"})
    public void testParseDeprecatedSnapshot(String snapshotOption) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        MetadataShell.MetadataShellArguments arguments = MetadataShell.parseArguments(
            new String[] {snapshotOption, "metadata.checkpoint"},
            new PrintStream(stream, true, StandardCharsets.UTF_8));

        assertEquals("metadata.checkpoint", arguments.checkpointPath());
        assertEquals(List.of(), arguments.command());
        assertTrue(stream.toString(StandardCharsets.UTF_8).contains(
            "Option --snapshot is deprecated and will be removed in a future version. Use --checkpoint instead."));
    }

    @Test
    public void testParseCheckpointAndDeprecatedSnapshotFails() {
        ArgumentParserException exception = assertThrows(ArgumentParserException.class, () ->
            MetadataShell.parseArguments(
                new String[] {
                    "--checkpoint", "metadata.checkpoint",
                    "--snapshot", "metadata.snapshot"
                },
                System.out));

        assertTrue(exception.getMessage().contains("not allowed with argument"));
    }

    @Test
    public void testParseWithoutCheckpointFails() {
        ArgumentParserException exception = assertThrows(ArgumentParserException.class, () ->
            MetadataShell.parseArguments(new String[] {"ls", "/"}, System.out));

        assertTrue(exception.getMessage().contains("required"));
    }
}
