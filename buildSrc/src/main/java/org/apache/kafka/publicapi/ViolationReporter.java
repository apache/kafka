package org.apache.kafka.publicapi;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generates reports for public API violations.
 */
public class ViolationReporter {

    /**
     * Write violations to a text report file.
     */
    public void writeTextReport(List<PublicApiViolation> violations, File reportFile) throws IOException {
        reportFile.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(reportFile))) {
            writer.println("Apache Kafka Public API Violation Report");
            writer.println("========================================");
            writer.println("Generated: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            writer.println("Total violations: " + violations.size());
            writer.println();

            if (violations.isEmpty()) {
                writer.println("No violations found! ✅");
                return;
            }

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
    }

    /**
     * Write violations to a JSON report file.
     */
    public void writeJsonReport(List<PublicApiViolation> violations, File reportFile) throws IOException {
        reportFile.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(reportFile))) {
            writer.println("{");
            writer.println("  \"timestamp\": \"" + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "\",");
            writer.println("  \"totalViolations\": " + violations.size() + ",");
            writer.println("  \"violations\": [");

            for (int i = 0; i < violations.size(); i++) {
                PublicApiViolation violation = violations.get(i);
                writer.println("    {");
                writer.println("      \"className\": \"" + escapeJson(violation.getClassName()) + "\",");
                writer.println("      \"violationType\": \"" + escapeJson(violation.getViolationType()) + "\",");
                writer.println("      \"description\": \"" + escapeJson(violation.getDescription()) + "\",");
                writer.println("      \"memberName\": " + (violation.getMemberName() != null ?
                    "\"" + escapeJson(violation.getMemberName()) + "\"" : "null"));
                writer.print("    }");
                if (i < violations.size() - 1) {
                    writer.println(",");
                } else {
                    writer.println();
                }
            }

            writer.println("  ],");

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

    /**
     * Print violations to console with color coding (if supported).
     */
    public void printToConsole(List<PublicApiViolation> violations, boolean useColors) {
        if (violations.isEmpty()) {
            System.out.println(useColors ? "\u001B[32m✅ No public API violations found!\u001B[0m" : "✅ No public API violations found!");
            return;
        }

        String redColor = useColors ? "\u001B[31m" : "";
        String yellowColor = useColors ? "\u001B[33m" : "";
        String resetColor = useColors ? "\u001B[0m" : "";

        System.out.println(redColor + "❌ Found " + violations.size() + " public API violations:" + resetColor);
        System.out.println();

        for (PublicApiViolation violation : violations) {
            System.out.println(yellowColor + violation.toString() + resetColor);
        }

        System.out.println();
        System.out.println("Please fix these violations to ensure API compatibility.");
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