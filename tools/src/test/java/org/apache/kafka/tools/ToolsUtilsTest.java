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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ToolsUtilsTest {

    @ParameterizedTest
    @NullAndEmptySource
    public void shouldNotPrintMetricsWhenMetricsAreNullOrEmpty(final Map<MetricName, Metric> metrics) {
        assertEquals("", ToolsTestUtils.captureStandardOut(() -> ToolsUtils.printMetrics(metrics)));
    }

    @Test
    public void shouldPrintMetricsInSortedOrderWithTypeSpecificFormatting() {
        final MetricName firstMetricName = new MetricName("first", "group", "first metric", Map.of());
        final MetricName secondMetricName = new MetricName("second", "group", "second metric", Map.of());
        final Map<MetricName, Metric> metrics = new LinkedHashMap<>();
        metrics.put(secondMetricName, metric(secondMetricName, "not-a-number"));
        metrics.put(firstMetricName, metric(firstMetricName, 1.23456d));

        final String output = ToolsTestUtils.captureStandardOut(() -> ToolsUtils.printMetrics(metrics));

        assertTrue(output.startsWith("Metric Name"));
        assertTrue(output.contains("group:first:{}  : 1.235"));
        assertTrue(output.contains("group:second:{} : not-a-number"));
        assertTrue(output.indexOf("group:first:{}") < output.indexOf("group:second:{}"));
    }

    @Test
    public void shouldPrintTableWithAlignedColumns() {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            ToolsUtils.prettyPrintTable(
                List.of("Name", "Value"),
                List.of(List.of("short", "1"), List.of("longer", "22")),
                out
            );
        }

        assertEquals("Name  \tValue\t\nshort \t1    \t\nlonger\t22   \t\n", output.toString(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost:9092", "host1:9091,host2:9092", "[::1]:9092"})
    public void shouldAcceptValidBootstrapServer(final String hostPort) {
        ToolsUtils.validateBootstrapServer(hostPort);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"localhost", "localhost:not-a-port", "localhost:9092,invalid"})
    public void shouldRejectInvalidBootstrapServer(final String hostPort) {
        assertThrows(IllegalArgumentException.class, () -> ToolsUtils.validateBootstrapServer(hostPort));
    }

    @Test
    public void shouldReturnEachDuplicateOnlyOnce() {
        assertEquals(Set.of("alpha", "beta"), ToolsUtils.duplicates(List.of("alpha", "beta", "alpha", "beta", "beta")));
        assertEquals(Set.of(), ToolsUtils.duplicates(List.of("alpha", "beta")));
    }

    @Test
    public void shouldReturnCopyWithoutSpecifiedElements() {
        final Set<String> source = Set.of("alpha", "beta", "gamma");

        final Set<String> result = ToolsUtils.minus(source, "beta", "missing", "beta");

        assertEquals(Set.of("alpha", "gamma"), result);
        assertEquals(Set.of("alpha", "beta", "gamma"), source);
        assertFalse(result == source);
    }

    private static Metric metric(final MetricName metricName, final Object value) {
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
}
