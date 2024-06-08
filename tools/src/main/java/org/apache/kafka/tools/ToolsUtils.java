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

import joptsimple.OptionParser;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ToolsUtils {
    /**
     * print out the metrics in alphabetical order
     * @param metrics   the metrics to be printed out
     */
    public static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
        if (metrics != null && !metrics.isEmpty()) {
            int maxLengthOfDisplayName = 0;
            TreeMap<String, Object> sortedMetrics = new TreeMap<>();
            for (Metric metric : metrics.values()) {
                MetricName mName = metric.metricName();
                String mergedName = mName.group() + ":" + mName.name() + ":" + mName.tags();
                maxLengthOfDisplayName = Math.max(maxLengthOfDisplayName, mergedName.length());
                sortedMetrics.put(mergedName, metric.metricValue());
            }
            String doubleOutputFormat = "%-" + maxLengthOfDisplayName + "s : %.3f";
            String defaultOutputFormat = "%-" + maxLengthOfDisplayName + "s : %s";
            System.out.printf("\n%-" + maxLengthOfDisplayName + "s   %s%n", "Metric Name", "Value");

            for (Map.Entry<String, Object> entry : sortedMetrics.entrySet()) {
                String outputFormat;
                if (entry.getValue() instanceof Double)
                    outputFormat = doubleOutputFormat;
                else
                    outputFormat = defaultOutputFormat;
                System.out.printf(outputFormat + "%n", entry.getKey(), entry.getValue());
            }
        }
    }

    private static void appendColumnValue(
        StringBuilder rowBuilder,
        String value,
        int length
    ) {
        int padLength = length - value.length();
        rowBuilder.append(value);
        for (int i = 0; i < padLength; i++)
            rowBuilder.append(' ');
    }

    private static void printRow(
        List<Integer> columnLengths,
        List<String> row,
        PrintStream out
    ) {
        StringBuilder rowBuilder = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            Integer columnLength = columnLengths.get(i);
            String columnValue = row.get(i);
            appendColumnValue(rowBuilder, columnValue, columnLength);
            rowBuilder.append('\t');
        }
        out.println(rowBuilder);
    }

    public static void prettyPrintTable(
        List<String> headers,
        List<List<String>> rows,
        PrintStream out
    ) {
        List<Integer> columnLengths = headers.stream()
            .map(String::length)
            .collect(Collectors.toList());

        for (List<String> row : rows) {
            for (int i = 0; i < headers.size(); i++) {
                columnLengths.set(i, Math.max(columnLengths.get(i), row.get(i).length()));
            }
        }

        printRow(columnLengths, headers, out);
        rows.forEach(row -> printRow(columnLengths, row, out));
    }

    public static void validateBootstrapServer(String hostPort) throws IllegalArgumentException {
        if (hostPort == null || hostPort.trim().isEmpty()) {
            throw new IllegalArgumentException("Error while validating the bootstrap address\n");
        }

        String[] hostPorts;

        if (hostPort.contains(",")) {
            hostPorts = hostPort.split(",");
        } else {
            hostPorts = new String[] {hostPort};
        }

        String[] validHostPort = Arrays.stream(hostPorts)
                .filter(hostPortData -> Utils.getPort(hostPortData) != null)
                .toArray(String[]::new);

        if (validHostPort.length == 0 || validHostPort.length != hostPorts.length) {
            throw new IllegalArgumentException("Please provide valid host:port like host1:9091,host2:9092\n");
        }
    }

    /**
     * Return all duplicates in a list. A duplicated element will appear only once.
     */
    public static <T> Set<T> duplicates(List<T> s) {
        Set<T> set = new HashSet<>();
        Set<T> duplicates = new HashSet<>();

        s.forEach(element -> {
            if (!set.add(element)) {
                duplicates.add(element);
            }
        });
        return duplicates;
    }

    /**
     * @param set Source set.
     * @param toRemove Elements to remove.
     * @return {@code set} copy without {@code toRemove} elements.
     * @param <T> Element type.
     */
    @SuppressWarnings("unchecked")
    public static <T> Set<T> minus(Set<T> set, T...toRemove) {
        Set<T> res = new HashSet<>(set);
        for (T t : toRemove)
            res.remove(t);
        return res;
    }

    /**
     * This is a simple wrapper around `CommandLineUtils.printUsageAndExit`.
     * It is needed for tools migration (KAFKA-14525), as there is no Java equivalent for return type `Nothing`.
     * Can be removed once [[kafka.tools.ConsoleConsumer]]
     * and [[kafka.tools.ConsoleProducer]] are migrated.
     *
     * @param parser Command line options parser.
     * @param message Error message.
     */
    public static void printUsageAndExit(OptionParser parser, String message) {
        CommandLineUtils.printUsageAndExit(parser, message);
        throw new AssertionError("printUsageAndExit should not return, but it did.");
    }
}
