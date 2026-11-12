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
package org.apache.kafka.apicheck;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generates reports for public API violations.
 */
public class ViolationReporter {

    /**
     * Write violations + suppressions to a text report file. Suppressions are rendered in a
     * dedicated section so reviewers can audit every place {@code @SuppressKafkaInternalApiUsage}
     * has been applied — together with the reason supplied to the annotation.
     */
    public void writeTextReport(List<PublicApiViolation> violations,
                                List<PublicApiViolation> suppressions,
                                File reportFile) throws IOException {
        reportFile.getParentFile().mkdirs();
        List<PublicApiViolation> safeSuppressions =
                suppressions == null ? Collections.emptyList() : suppressions;

        // Report contents must be reproducible: any two runs over the same inputs should produce
        // byte-identical reports so CI diff tooling and reviewers can compare cleanly. That rules
        // out a wall-clock timestamp (omitted) and HashMap-order grouping (replaced with TreeMap
        // + a stable per-list sort by class then member).
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(reportFile.toPath(), StandardCharsets.UTF_8))) {
            writer.println("Apache Kafka Public API Violation Report");
            writer.println("========================================");
            writer.println("Total violations: " + violations.size());
            writer.println("Total suppressions: " + safeSuppressions.size());
            writer.println();

            if (violations.isEmpty()) {
                writer.println("No violations found.");
            } else {
                Map<String, List<PublicApiViolation>> violationsByType = groupSorted(
                        violations, PublicApiViolation::getViolationType);
                for (Map.Entry<String, List<PublicApiViolation>> entry : violationsByType.entrySet()) {
                    writer.println("## " + entry.getKey() + " (" + entry.getValue().size() + " violations)");
                    writer.println();
                    for (PublicApiViolation violation : entry.getValue()) {
                        writer.println("- " + violation.toString());
                    }
                    writer.println();
                }

                writer.println("## Summary by Class");
                writer.println();
                Map<String, List<PublicApiViolation>> violationsByClass = groupSorted(
                        violations, PublicApiViolation::getClassName);
                for (Map.Entry<String, List<PublicApiViolation>> entry : violationsByClass.entrySet()) {
                    writer.println("### " + entry.getKey() + " (" + entry.getValue().size() + " violations)");
                    for (PublicApiViolation violation : entry.getValue()) {
                        writer.println("  - " + violation.getViolationType() + ": " + violation.getDescription());
                    }
                    writer.println();
                }
            }

            if (!safeSuppressions.isEmpty()) {
                writer.println("## Suppressions (" + safeSuppressions.size() + " entries)");
                writer.println("Checks skipped due to @SuppressKafkaInternalApiUsage.");
                writer.println("Each line shows the reason supplied to the annotation; review periodically.");
                writer.println();
                List<PublicApiViolation> sortedSuppressions = new ArrayList<>(safeSuppressions);
                sortedSuppressions.sort(VIOLATION_ORDER);
                for (PublicApiViolation suppression : sortedSuppressions) {
                    writer.println("- " + suppression.getDescription());
                }
                writer.println();
            }
        }
    }

    /** Stable sort key for any list of violations: class, then member, then description. */
    private static final Comparator<PublicApiViolation> VIOLATION_ORDER =
            Comparator.comparing(PublicApiViolation::getClassName, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(PublicApiViolation::getMemberName, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(PublicApiViolation::getDescription, Comparator.nullsFirst(String::compareTo));

    private static <K extends Comparable<? super K>> Map<K, List<PublicApiViolation>> groupSorted(
            List<PublicApiViolation> violations,
            java.util.function.Function<PublicApiViolation, K> keyFn) {
        Map<K, List<PublicApiViolation>> grouped = violations.stream()
                .collect(Collectors.groupingBy(keyFn, TreeMap::new, Collectors.toList()));
        grouped.values().forEach(list -> list.sort(VIOLATION_ORDER));
        return grouped;
    }

    /** Back-compat overload — call sites that don't yet pass suppressions. */
    public void writeTextReport(List<PublicApiViolation> violations, File reportFile) throws IOException {
        writeTextReport(violations, Collections.emptyList(), reportFile);
    }

    /**
     * Print to console using {@link #shouldUseColors()} to decide whether to emit ANSI escapes.
     * Callers in unknown environments (Gradle / Maven tasks that may run under CI / redirected
     * output) should prefer this overload over hard-coding {@code useColors=true}.
     */
    public void printToConsole(List<PublicApiViolation> violations,
                               List<PublicApiViolation> suppressions) {
        printToConsole(violations, suppressions, shouldUseColors());
    }

    /**
     * True when stdout is attached to a real terminal and the user hasn't opted out via the
     * {@code NO_COLOR} convention (<a href="https://no-color.org/">no-color.org</a>). Returns
     * false under CI or when output has been redirected, so colored output doesn't leak ANSI
     * escapes into logs/files.
     */
    public static boolean shouldUseColors() {
        if (System.getenv("NO_COLOR") != null) return false;
        return System.console() != null;
    }

    /**
     * Print violations to console with color coding (if supported). Suppressions are listed at the
     * end so reviewers see what was waived (each with reason).
     */
    public void printToConsole(List<PublicApiViolation> violations,
                               List<PublicApiViolation> suppressions,
                               boolean useColors) {
        String redColor = useColors ? "\u001B[31m" : "";
        String greenColor = useColors ? "\u001B[32m" : "";
        String yellowColor = useColors ? "\u001B[33m" : "";
        String cyanColor = useColors ? "\u001B[36m" : "";
        String resetColor = useColors ? "\u001B[0m" : "";

        if (violations.isEmpty()) {
            System.out.println(greenColor + "No public API violations found." + resetColor);
        } else {
            System.out.println(redColor + "Found " + violations.size() + " public API violation(s):" + resetColor);
            System.out.println();
            for (PublicApiViolation violation : violations) {
                System.out.println(yellowColor + violation.toString() + resetColor);
            }
            System.out.println();
            System.out.println("Please fix these violations to ensure API compatibility.");
        }

        if (suppressions != null && !suppressions.isEmpty()) {
            System.out.println();
            System.out.println(cyanColor + suppressions.size()
                    + " check(s) suppressed via @SuppressKafkaInternalApiUsage:" + resetColor);
            for (PublicApiViolation suppression : suppressions) {
                System.out.println(cyanColor + "  " + suppression.getDescription() + resetColor);
            }
        }
    }

}
