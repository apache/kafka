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

package org.apache.kafka.message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class HeaderVersionsTest {

    private static MessageSpec parse(String spec) throws Exception {
        return MessageGenerator.JSON_SERDE.readValue(spec.replace('\'', '"'), MessageSpec.class);
    }

    private static String requestSpec(String validVersions, String flexibleVersions, String headerVersions) {
        return "{'apiKey': 0, 'type': 'request', 'name': 'FooRequest', 'validVersions': '" + validVersions +
            "', 'flexibleVersions': '" + flexibleVersions + "'" +
            (headerVersions == null ? "" : ", 'headerVersions': " + headerVersions) + "}";
    }

    private static Map<String, String> map(String... keyValues) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            result.put(keyValues[i], keyValues[i + 1]);
        }
        return result;
    }

    private static void assertMessageContains(String expectedSubstring, Executable executable) {
        Exception exception = assertThrows(Exception.class, executable);
        StringBuilder messages = new StringBuilder();
        for (Throwable throwable = exception; throwable != null; throwable = throwable.getCause()) {
            if (throwable.getMessage() != null) {
                messages.append(throwable.getMessage()).append(" | ");
            }
        }
        assertTrue(messages.toString().contains(expectedSubstring),
            "Expected an exception message containing \"" + expectedSubstring + "\", but was: " + messages);
    }

    @Test
    public void testSingleRange() throws Exception {
        MessageSpec spec = parse(requestSpec("0-5", "0+", "{'0+': '2'}"));
        List<HeaderVersions.Entry> entries = spec.headerVersions().orElseThrow().entries();
        assertEquals(1, entries.size());
        assertEquals("0+", entries.get(0).range().toString());
        assertEquals((short) 2, entries.get(0).headerVersion());
    }

    @Test
    public void testTwoRanges() throws Exception {
        MessageSpec spec = parse(requestSpec("0-5", "2+", "{'0-1': '1', '2+': '2'}"));
        List<HeaderVersions.Entry> entries = spec.headerVersions().orElseThrow().entries();
        assertEquals(2, entries.size());
        assertEquals("0-1", entries.get(0).range().toString());
        assertEquals((short) 1, entries.get(0).headerVersion());
        assertEquals("2+", entries.get(1).range().toString());
        assertEquals((short) 2, entries.get(1).headerVersion());
    }

    @Test
    public void testSingleVersionKey() throws Exception {
        MessageSpec spec = parse(requestSpec("0-5", "1+", "{'0': '1', '1+': '2'}"));
        List<HeaderVersions.Entry> entries = spec.headerVersions().orElseThrow().entries();
        assertEquals(2, entries.size());
        assertEquals("0", entries.get(0).range().toString());
        assertEquals("1+", entries.get(1).range().toString());
    }

    @Test
    public void testNonZeroStart() throws Exception {
        MessageSpec spec = parse(requestSpec("3-9", "9+", "{'3-8': '1', '9+': '2'}"));
        List<HeaderVersions.Entry> entries = spec.headerVersions().orElseThrow().entries();
        assertEquals(2, entries.size());
        assertEquals("3-8", entries.get(0).range().toString());
        assertEquals("9+", entries.get(1).range().toString());
    }

    @Test
    public void testEntriesAreSortedAscending() throws Exception {
        MessageSpec spec = parse(requestSpec("0-5", "2+", "{'2+': '2', '0-1': '1'}"));
        List<HeaderVersions.Entry> entries = spec.headerVersions().orElseThrow().entries();
        assertEquals("0-1", entries.get(0).range().toString());
        assertEquals("2+", entries.get(1).range().toString());
    }

    @Test
    public void testToMapRoundTrip() throws Exception {
        MessageSpec spec = parse(requestSpec("0-5", "2+", "{'0-1': '1', '2+': '2'}"));
        assertEquals(map("0-1", "1", "2+", "2"), spec.headerVersions().orElseThrow().toMap());
        assertEquals(map("0-1", "1", "2+", "2"), spec.headerVersionsStrings());
    }

    @Test
    public void testAbsentProperty() throws Exception {
        MessageSpec spec = parse(requestSpec("0-5", "2+", null));
        assertTrue(spec.headerVersions().isEmpty());
        assertTrue(spec.headerVersionsStrings() == null);
    }

    @Test
    public void testEmptyMap() {
        assertMessageContains("empty headerVersions", () -> parse(requestSpec("0-5", "2+", "{}")));
    }

    @Test
    public void testBlankKey() {
        assertMessageContains("blank version range", () -> parse(requestSpec("0-5", "2+", "{'': '1'}")));
    }

    @Test
    public void testUnparseableKey() {
        assertMessageContains("invalid version range", () -> parse(requestSpec("0-5", "2+", "{'abc': '1'}")));
    }

    @Test
    public void testNoneKey() {
        assertMessageContains("invalid version range", () -> parse(requestSpec("0-5", "2+", "{'none': '1'}")));
    }

    @Test
    public void testReversedRangeKey() {
        assertMessageContains("invalid version range", () -> parse(requestSpec("0-5", "2+", "{'5-3': '1'}")));
    }

    @Test
    public void testNegativeValue() {
        assertMessageContains("negative header version", () -> parse(requestSpec("0-5", "2+", "{'0+': '-1'}")));
    }

    @Test
    public void testUnparseableValue() {
        assertMessageContains("invalid header version", () -> parse(requestSpec("0-5", "2+", "{'0+': 'x'}")));
    }

    @Test
    public void testDoesNotStartAtLowestValidVersion() {
        assertMessageContains("lowest valid version", () -> parse(requestSpec("0-9", "2+", "{'2+': '2'}")));
    }

    @Test
    public void testGap() {
        assertMessageContains("non-contiguous", () -> parse(requestSpec("0-9", "3+", "{'0-1': '1', '3+': '2'}")));
    }

    @Test
    public void testOverlap() {
        assertMessageContains("non-contiguous", () -> parse(requestSpec("0-9", "2+", "{'0-2': '1', '2+': '2'}")));
    }

    @Test
    public void testLastRangeNotOpenEnded() {
        assertMessageContains("open-ended", () -> parse(requestSpec("0-9", "2+", "{'0-1': '1', '2-9': '2'}")));
    }

    @Test
    public void testRangeBeyondValidVersions() {
        assertMessageContains("above the highest valid version",
            () -> parse(requestSpec("0-2", "3+", "{'0-2': '1', '3+': '2'}")));
    }

    @Test
    public void testPropertyOnDataType() {
        assertMessageContains("only valid for messages with type",
            () -> parse("{'type': 'data', 'name': 'FooData', 'validVersions': '0-2', " +
                "'flexibleVersions': '0+', 'headerVersions': {'0+': '2'}}"));
    }

    @Test
    public void testPropertyWithNoValidVersions() {
        assertMessageContains("no valid versions",
            () -> parse("{'apiKey': 0, 'type': 'request', 'name': 'FooRequest', 'validVersions': 'none', " +
                "'headerVersions': {'0+': '1'}}"));
    }
}
