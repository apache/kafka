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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ToolsUtilsTest {

    private static Metric metric(String group, String name, Object value) {
        MetricName metricName = new MetricName(name, group, "desc", Collections.emptyMap());
        return new Metric() {
            @Override
            public MetricName metricName() {
                return metricName;
            }

            @Override
            public Object metricValue() {
                return value;
            }
        };
    }

    /**
     * Run the given action while capturing everything written to {@code System.out},
     * restoring the original stream afterwards.
     */
    private static String captureStdout(Runnable action) {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
            action.run();
        } finally {
            System.setOut(originalOut);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @Test
    public void testPrintMetricsPrintsSortedWithHeader() {
        Map<MetricName, Metric> metrics = new LinkedHashMap<>();
        // Deliberately insert out of alphabetical order to verify sorting.
        Metric zebra = metric("groupZ", "zebra", 1.0);
        Metric alpha = metric("groupA", "alpha", "someString");
        metrics.put(zebra.metricName(), zebra);
        metrics.put(alpha.metricName(), alpha);

        String output = captureStdout(() -> ToolsUtils.printMetrics(metrics));

        assertTrue(output.contains("Metric Name"), "output should contain the header");
        assertTrue(output.contains("groupA:alpha"), "output should contain the alpha metric");
        assertTrue(output.contains("groupZ:zebra"), "output should contain the zebra metric");
        // alpha should be printed before zebra because metrics are sorted alphabetically.
        assertTrue(output.indexOf("groupA:alpha") < output.indexOf("groupZ:zebra"),
            "metrics should be printed in alphabetical order");
        // Double values are formatted with three decimal places.
        assertTrue(output.contains("1.000"), "double values should be formatted with 3 decimals");
        // Non-double values are printed as-is via the default "%s" format.
        assertTrue(output.contains("someString"), "non-double values should be printed as-is");
    }

    @Test
    public void testPrintMetricsWithNullOrEmptyPrintsNothing() {
        assertEquals("", captureStdout(() -> ToolsUtils.printMetrics(null)));
        assertEquals("", captureStdout(() -> ToolsUtils.printMetrics(Collections.emptyMap())));
    }

    @Test
    public void testPrettyPrintTable() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8);

        List<String> headers = Arrays.asList("NAME", "VALUE");
        List<List<String>> rows = Arrays.asList(
            Arrays.asList("a", "1"),
            Arrays.asList("longer-name", "22"));

        ToolsUtils.prettyPrintTable(headers, rows, out);
        String output = buffer.toString(StandardCharsets.UTF_8);

        assertTrue(output.contains("NAME"), "output should contain the header NAME");
        assertTrue(output.contains("VALUE"), "output should contain the header VALUE");
        assertTrue(output.contains("longer-name"), "output should contain the second row value");
        // Header appears before the data rows.
        assertTrue(output.indexOf("NAME") < output.indexOf("longer-name"),
            "header row should be printed before the data rows");
        // Every row's first column is padded to the width of the widest cell
        // ("longer-name" = 11 chars), which is the core behaviour of prettyPrintTable.
        for (String line : output.split(System.lineSeparator())) {
            if (line.isEmpty()) {
                continue;
            }
            String firstColumn = line.substring(0, line.indexOf('\t'));
            assertEquals(11, firstColumn.length(),
                "first column should be padded to the widest value's width: '" + line + "'");
        }
    }

    @Test
    public void testPrettyPrintTableWithNoRows() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8);

        ToolsUtils.prettyPrintTable(Arrays.asList("H1", "H2"), Collections.emptyList(), out);
        String output = buffer.toString(StandardCharsets.UTF_8);

        assertTrue(output.contains("H1"));
        assertTrue(output.contains("H2"));
    }

    @Test
    public void testValidateBootstrapServerWithValidInput() {
        assertDoesNotThrow(() -> ToolsUtils.validateBootstrapServer("localhost:9092"));
        assertDoesNotThrow(() -> ToolsUtils.validateBootstrapServer("host1:9091,host2:9092"));
    }

    @Test
    public void testValidateBootstrapServerWithInvalidInput() {
        // null, empty and blank are rejected.
        assertThrows(IllegalArgumentException.class, () -> ToolsUtils.validateBootstrapServer(null));
        assertThrows(IllegalArgumentException.class, () -> ToolsUtils.validateBootstrapServer(""));
        assertThrows(IllegalArgumentException.class, () -> ToolsUtils.validateBootstrapServer("   "));
        // missing port.
        assertThrows(IllegalArgumentException.class, () -> ToolsUtils.validateBootstrapServer("localhost"));
        // one valid and one invalid entry is still rejected.
        assertThrows(IllegalArgumentException.class, () -> ToolsUtils.validateBootstrapServer("host1:9091,invalid"));
    }

    @Test
    public void testDuplicates() {
        Set<Integer> result = ToolsUtils.duplicates(Arrays.asList(1, 2, 2, 3, 3, 3));
        assertEquals(new HashSet<>(Arrays.asList(2, 3)), result);
    }

    @Test
    public void testDuplicatesWithNoDuplicates() {
        assertTrue(ToolsUtils.duplicates(Arrays.asList(1, 2, 3)).isEmpty());
        assertTrue(ToolsUtils.duplicates(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testMinus() {
        Set<Integer> source = new HashSet<>(Arrays.asList(1, 2, 3, 4));

        assertEquals(new HashSet<>(Arrays.asList(1, 3, 4)), ToolsUtils.minus(source, 2));
        assertEquals(new HashSet<>(Arrays.asList(1, 4)), ToolsUtils.minus(source, 2, 3));
        // Removing nothing returns an equal copy.
        assertEquals(source, ToolsUtils.minus(source));
        // Removing an absent element leaves the set unchanged.
        assertEquals(source, ToolsUtils.minus(source, 99));
        // The original set must not be modified.
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 3, 4)), source);
    }
}
