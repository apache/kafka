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
package org.apache.kafka.publicapi;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

        try (PrintWriter writer = new PrintWriter(new FileWriter(reportFile))) {
            writer.println("Apache Kafka Public API Violation Report");
            writer.println("========================================");
            writer.println("Generated: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            writer.println("Total violations: " + violations.size());
            writer.println("Total suppressions: " + safeSuppressions.size());
            writer.println();

            if (violations.isEmpty()) {
                writer.println("No violations found! ✅");
            } else {
                // Group violations by type
                Map<String, List<PublicApiViolation>> violationsByType = violations.stream()
                    .collect(Collectors.groupingBy(PublicApiViolation::getViolationType));

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

                // Group by class name
                Map<String, List<PublicApiViolation>> violationsByClass = violations.stream()
                    .collect(Collectors.groupingBy(PublicApiViolation::getClassName));

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
                for (PublicApiViolation suppression : safeSuppressions) {
                    writer.println("- " + suppression.getDescription());
                }
                writer.println();
            }
        }
    }

    /** Back-compat overload — call sites that don't yet pass suppressions. */
    public void writeTextReport(List<PublicApiViolation> violations, File reportFile) throws IOException {
        writeTextReport(violations, Collections.emptyList(), reportFile);
    }

    /**
     * Write violations + suppressions to a JSON report file.
     */
    public void writeJsonReport(List<PublicApiViolation> violations,
                                List<PublicApiViolation> suppressions,
                                File reportFile) throws IOException {
        reportFile.getParentFile().mkdirs();
        List<PublicApiViolation> safeSuppressions =
                suppressions == null ? Collections.emptyList() : suppressions;

        try (PrintWriter writer = new PrintWriter(new FileWriter(reportFile))) {
            writer.println("{");
            writer.println("  \"timestamp\": \"" + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "\",");
            writer.println("  \"totalViolations\": " + violations.size() + ",");
            writer.println("  \"totalSuppressions\": " + safeSuppressions.size() + ",");
            writeJsonEntries(writer, "violations", violations);
            writer.println(",");
            writeJsonEntries(writer, "suppressions", safeSuppressions);
            writer.println(",");

            // Summary statistics
            Map<String, Long> violationCounts = violations.stream()
                .collect(Collectors.groupingBy(PublicApiViolation::getViolationType, Collectors.counting()));

            writer.println("  \"summary\": {");
            int count = 0;
            for (Map.Entry<String, Long> entry : violationCounts.entrySet()) {
                writer.print("    \"" + escapeJson(entry.getKey()) + "\": " + entry.getValue());
                if (++count < violationCounts.size()) {
                    writer.println(",");
                } else {
                    writer.println();
                }
            }
            writer.println("  }");
            writer.println("}");
        }
    }

    /** Back-compat overload. */
    public void writeJsonReport(List<PublicApiViolation> violations, File reportFile) throws IOException {
        writeJsonReport(violations, Collections.emptyList(), reportFile);
    }

    private void writeJsonEntries(PrintWriter writer, String name, List<PublicApiViolation> entries) {
        writer.println("  \"" + name + "\": [");
        for (int i = 0; i < entries.size(); i++) {
            PublicApiViolation v = entries.get(i);
            writer.println("    {");
            writer.println("      \"className\": \"" + escapeJson(v.getClassName()) + "\",");
            writer.println("      \"violationType\": \"" + escapeJson(v.getViolationType()) + "\",");
            writer.println("      \"description\": \"" + escapeJson(v.getDescription()) + "\",");
            writer.println("      \"memberName\": " + (v.getMemberName() != null ?
                "\"" + escapeJson(v.getMemberName()) + "\"" : "null"));
            writer.print("    }");
            if (i < entries.size() - 1) {
                writer.println(",");
            } else {
                writer.println();
            }
        }
        writer.print("  ]");
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
            System.out.println(greenColor + "✅ No public API violations found!" + resetColor);
        } else {
            System.out.println(redColor + "❌ Found " + violations.size() + " public API violations:" + resetColor);
            System.out.println();
            for (PublicApiViolation violation : violations) {
                System.out.println(yellowColor + violation.toString() + resetColor);
            }
            System.out.println();
            System.out.println("Please fix these violations to ensure API compatibility.");
        }

        if (suppressions != null && !suppressions.isEmpty()) {
            System.out.println();
            System.out.println(cyanColor + "ℹ " + suppressions.size()
                    + " check(s) suppressed via @SuppressKafkaInternalApiUsage:" + resetColor);
            for (PublicApiViolation suppression : suppressions) {
                System.out.println(cyanColor + "  " + suppression.getDescription() + resetColor);
            }
        }
    }

    /** Back-compat overload. */
    public void printToConsole(List<PublicApiViolation> violations, boolean useColors) {
        printToConsole(violations, Collections.emptyList(), useColors);
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                   .replace("\"", "\\\"")
                   .replace("\n", "\\n")
                   .replace("\r", "\\r")
                   .replace("\t", "\\t");
    }
}
