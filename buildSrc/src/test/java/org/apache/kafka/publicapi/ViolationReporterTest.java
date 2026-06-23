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

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ViolationReporterTest {

    @Test
    void escapeJson_nullInput_returnsEmptyString() {
        assertEquals("", ViolationReporter.escapeJson(null));
    }

    @Test
    void escapeJson_handlesStandardShorthandEscapes() {
        assertEquals("a\\\\b", ViolationReporter.escapeJson("a\\b"));
        assertEquals("a\\\"b", ViolationReporter.escapeJson("a\"b"));
        assertEquals("a\\nb", ViolationReporter.escapeJson("a\nb"));
        assertEquals("a\\rb", ViolationReporter.escapeJson("a\rb"));
        assertEquals("a\\tb", ViolationReporter.escapeJson("a\tb"));
        assertEquals("a\\bb", ViolationReporter.escapeJson("a\bb"));
        assertEquals("a\\fb", ViolationReporter.escapeJson("a\fb"));
    }

    @Test
    void escapeJson_escapesOtherControlCharsAsUnicodeEscape() {
        // A control char with no shorthand escape would otherwise land raw in the JSON string
        // and break the document. RFC 8259 requires the six-character backslash-u-XXXX form.
        assertEquals("a\\u0001b", ViolationReporter.escapeJson("a\u0001b"));
        assertEquals("a\\u0000b", ViolationReporter.escapeJson("a\u0000b"));
        assertEquals("a\\u001fb", ViolationReporter.escapeJson("a\u001fb"));
    }

    @Test
    void escapeJson_leavesNormalCharsAlone() {
        assertEquals("hello world", ViolationReporter.escapeJson("hello world"));
        // 0x20 (space) is the first non-control codepoint and stays raw.
        assertEquals(" ", ViolationReporter.escapeJson(" "));
    }

    @Test
    void jsonReportFor_swapsTxtSuffixForJson() {
        File parent = new File("/tmp/reports");
        assertEquals(new File(parent, "report.json"),
                ViolationReporter.jsonReportFor(new File(parent, "report.txt")));
    }

    @Test
    void jsonReportFor_appendsJsonWhenSuffixNotTxt() {
        // Anchored at end: an intermediate ".txt" must not be touched.
        File parent = new File("/tmp/reports");
        assertEquals(new File(parent, "report.txt.bak.json"),
                ViolationReporter.jsonReportFor(new File(parent, "report.txt.bak")));
    }

    @Test
    void jsonReportFor_onlyReplacesTrailingTxt() {
        // The old `replace(".txt", ".json")` would mangle this name; the new helper only
        // strips a trailing `.txt`, leaving intermediate `.txt` segments alone.
        File parent = new File("/tmp/reports");
        assertEquals(new File(parent, "my.txt.report.json"),
                ViolationReporter.jsonReportFor(new File(parent, "my.txt.report.txt")));
    }
}