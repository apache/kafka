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
package org.apache.kafka.tools;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ToolsUtilsTest {

    // --- validateBootstrapServer ---

    @Test
    public void testValidateBootstrapServerSingleHost() {
        ToolsUtils.validateBootstrapServer("localhost:9092");
    }

    @Test
    public void testValidateBootstrapServerMultipleHosts() {
        ToolsUtils.validateBootstrapServer("host1:9091,host2:9092,host3:9093");
    }

    @Test
    public void testValidateBootstrapServerNullThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> ToolsUtils.validateBootstrapServer(null));
    }

    @Test
    public void testValidateBootstrapServerEmptyThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> ToolsUtils.validateBootstrapServer(""));
    }

    @Test
    public void testValidateBootstrapServerBlankThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> ToolsUtils.validateBootstrapServer("   "));
    }

    @Test
    public void testValidateBootstrapServerNoPortThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> ToolsUtils.validateBootstrapServer("localhost"));
    }

    @Test
    public void testValidateBootstrapServerOneInvalidInListThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> ToolsUtils.validateBootstrapServer("host1:9091,badhost"));
    }

    // --- duplicates ---

    @Test
    public void testDuplicatesNone() {
        Set<Integer> result = ToolsUtils.duplicates(Arrays.asList(1, 2, 3));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDuplicatesSingle() {
        Set<String> result = ToolsUtils.duplicates(Arrays.asList("a", "b", "a"));
        assertEquals(Set.of("a"), result);
    }

    @Test
    public void testDuplicatesMultiple() {
        Set<Integer> result = ToolsUtils.duplicates(Arrays.asList(1, 2, 1, 3, 2, 2));
        assertEquals(Set.of(1, 2), result);
    }

    @Test
    public void testDuplicatesEmpty() {
        Set<Object> result = ToolsUtils.duplicates(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDuplicatesAllSame() {
        Set<String> result = ToolsUtils.duplicates(Arrays.asList("x", "x", "x"));
        assertEquals(Set.of("x"), result);
    }

    // --- minus ---

    @Test
    public void testMinusRemovesPresent() {
        Set<Integer> result = ToolsUtils.minus(Set.of(1, 2, 3), 2, 3);
        assertEquals(Set.of(1), result);
    }

    @Test
    public void testMinusIgnoresAbsent() {
        Set<String> result = ToolsUtils.minus(Set.of("a", "b"), "c");
        assertEquals(Set.of("a", "b"), result);
    }

    @Test
    public void testMinusEmptySet() {
        Set<Integer> result = ToolsUtils.minus(Collections.emptySet(), 1);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testMinusNoArgsReturnsFullCopy() {
        Set<String> original = Set.of("x", "y");
        Set<String> result = ToolsUtils.minus(original);
        assertEquals(original, result);
    }

    @Test
    public void testMinusDoesNotMutateOriginal() {
        Set<Integer> original = new java.util.HashSet<>(Arrays.asList(1, 2, 3));
        ToolsUtils.minus(original, 2);
        assertEquals(Set.of(1, 2, 3), original);
    }

    // --- prettyPrintTable ---

    @Test
    public void testPrettyPrintTableHeaders() {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ToolsUtils.prettyPrintTable(
            Arrays.asList("Name", "Value"),
            Collections.emptyList(),
            new PrintStream(bout)
        );
        String output = bout.toString();
        assertTrue(output.contains("Name"));
        assertTrue(output.contains("Value"));
    }

    @Test
    public void testPrettyPrintTableRows() {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ToolsUtils.prettyPrintTable(
            Arrays.asList("Topic", "Partition"),
            Arrays.asList(
                Arrays.asList("test-topic", "0"),
                Arrays.asList("other-topic", "1")
            ),
            new PrintStream(bout)
        );
        String output = bout.toString();
        assertTrue(output.contains("test-topic"));
        assertTrue(output.contains("other-topic"));
        assertTrue(output.contains("0"));
        assertTrue(output.contains("1"));
    }

    @Test
    public void testPrettyPrintTableColumnsAlignedByMaxWidth() {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        List<String> headers = List.of("H");
        List<List<String>> rows = Arrays.asList(
            List.of("short"),
            List.of("a-much-longer-value")
        );
        ToolsUtils.prettyPrintTable(headers, rows, new PrintStream(bout));
        String[] lines = bout.toString().split(System.lineSeparator());
        // header cell should be padded to match the longest row value
        assertTrue(lines[0].startsWith("H"));
        int headerCellEnd = lines[0].indexOf('\t');
        int rowCellEnd = lines[1].indexOf('\t');
        assertEquals(headerCellEnd, rowCellEnd,
            "All cells in a column should have the same padded width");
    }
}
