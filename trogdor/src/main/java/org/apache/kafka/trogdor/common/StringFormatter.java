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

package org.apache.kafka.trogdor.common;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Utilities for formatting strings.
 */
public class StringFormatter {
    /**
     * Pretty-print a date string.
     *
     * @param timeMs        The time since the epoch in milliseconds.
     * @param zoneOffset    The time zone offset.
     * @return              The date string in ISO format.
     */
    public static String dateString(long timeMs, ZoneOffset zoneOffset) {
        return new Date(timeMs).toInstant().
            atOffset(zoneOffset).
            format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /**
     * Pretty-print a duration.
     *
     * @param periodMs      The duration in milliseconds.
     * @return              A human-readable duration string.
     */
    public static String durationString(long periodMs) {
        StringBuilder bld = new StringBuilder();
        Duration duration = Duration.ofMillis(periodMs);
        long hours = duration.toHours();
        if (hours > 0) {
            bld.append(hours).append("h");
            duration = duration.minusHours(hours);
        }
        long minutes = duration.toMinutes();
        if (minutes > 0) {
            bld.append(minutes).append("m");
            duration = duration.minusMinutes(minutes);
        }
        long seconds = duration.getSeconds();
        if ((seconds != 0) || bld.toString().isEmpty()) {
            bld.append(seconds).append("s");
        }
        return bld.toString();
    }

    /**
     * Formats strings in a grid pattern.
     *
     * All entries in the same column will have the same width.
     *
     * @param lines     A list of lines.  Each line contains a list of columns.
     *                  Each line must contain the same number of columns.
     * @return          The string.
     */
    public static String prettyPrintGrid(List<List<String>> lines) {
        int numColumns = -1;
        int rowIndex = 0;
        for (List<String> col : lines) {
            if (numColumns == -1) {
                numColumns = col.size();
            } else if (numColumns != col.size()) {
                throw new RuntimeException("Expected " + numColumns + " columns in row " +
                    rowIndex + ", but got " + col.size());
            }
            rowIndex++;
        }
        List<Integer> widths = new ArrayList<>(numColumns);
        for (int x = 0; x < numColumns; x++) {
            int w = 0;
            for (List<String> cols : lines) {
                w = Math.max(w, cols.get(x).length() + 1);
            }
            widths.add(w);
        }
        StringBuilder bld = new StringBuilder();
        for (List<String> cols : lines) {
            for (int x = 0; x < cols.size(); x++) {
                String val = cols.get(x);
                int minWidth = widths.get(x);
                bld.append(val);
                for (int i = 0; i < minWidth - val.length(); i++) {
                    bld.append(" ");
                }
            }
            bld.append(String.format("%n"));
        }
        return bld.toString();
    }
}
