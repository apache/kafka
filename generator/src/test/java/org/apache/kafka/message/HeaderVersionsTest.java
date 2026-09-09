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

    private static String responseSpec(int apiKey, String name, String validVersions, String flexibleVersions,
                                       String headerVersions) {
        return "{'apiKey': " + apiKey + ", 'type': 'response', 'name': '" + name + "', 'validVersions': '" +
            validVersions + "', 'flexibleVersions': '" + flexibleVersions + "'" +
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
    public void testTruncatedValidVersionsStillStartAtZero() throws Exception {
        // Versions 0-2 are no longer valid, but the map still describes them, like flexibleVersions does.
        MessageSpec spec = parse(requestSpec("3-9", "9+", "{'0-8': '1', '9+': '2'}"));
        List<HeaderVersions.Entry> entries = spec.headerVersions().orElseThrow().entries();
        assertEquals(2, entries.size());
        assertEquals("0-8", entries.get(0).range().toString());
        assertEquals("9+", entries.get(1).range().toString());

        spec = parse(requestSpec("2-3", "0+", "{'0+': '2'}"));
        entries = spec.headerVersions().orElseThrow().entries();
        assertEquals(1, entries.size());
        assertEquals("0+", entries.get(0).range().toString());
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
    public void testNegativeVersionRangeKey() {
        assertMessageContains("invalid version range", () -> parse(requestSpec("0-5", "2+", "{'-5+': '1'}")));
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
    public void testNullValue() {
        assertMessageContains("blank header version", () -> parse(requestSpec("0-5", "2+", "{'0+': null}")));
    }

    @Test
    public void testBlankValue() {
        assertMessageContains("blank header version", () -> parse(requestSpec("0-5", "2+", "{'0+': ''}")));
    }

    @Test
    public void testDoesNotStartAtVersionZero() {
        assertMessageContains("must start at version 0", () -> parse(requestSpec("0-9", "2+", "{'2+': '2'}")));
        assertMessageContains("must start at version 0", () -> parse(requestSpec("2-3", "0+", "{'2+': '2'}")));
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
    public void testPropertyIgnoredWithNoValidVersions() throws Exception {
        // Like flexibleVersions, the property is ignored for a message with no valid versions.
        MessageSpec spec = parse("{'apiKey': 0, 'type': 'request', 'name': 'FooRequest', " +
            "'validVersions': 'none', 'headerVersions': {'0+': '1'}}");
        assertTrue(spec.headerVersions().isEmpty());
    }

    @Test
    public void testApiVersionsResponseMustUseHeaderVersionZero() {
        assertMessageContains("KIP-511",
            () -> parse(responseSpec(18, "ApiVersionsResponse", "0-3", "3+", "{'0-2': '0', '3+': '1'}")));
    }

    @Test
    public void testApiVersionsResponseHeaderVersionZeroAcceptedForFlexibleVersions() throws Exception {
        MessageSpec spec = parse(responseSpec(18, "ApiVersionsResponse", "0-3", "3+", "{'0+': '0'}"));
        assertEquals((short) 0, spec.headerVersions().orElseThrow().entries().get(0).headerVersion());
    }

    @Test
    public void testFlexibleRequestVersionNeedsFlexibleHeader() {
        assertMessageContains("which is flexible", () -> parse(requestSpec("0-5", "2+", "{'0+': '1'}")));
    }

    @Test
    public void testFlexibleRequestAcceptsNewerHeaderVersion() throws Exception {
        // Request header v3 (KIP-1313) is flexible, so a flexible body may declare it.
        MessageSpec spec = parse(requestSpec("0-5", "0+", "{'0+': '3'}"));
        assertEquals((short) 3, spec.headerVersions().orElseThrow().entries().get(0).headerVersion());
    }

    @Test
    public void testFlexibleResponseVersionNeedsFlexibleHeader() {
        assertMessageContains("which is flexible",
            () -> parse(responseSpec(0, "FooResponse", "0-5", "2+", "{'0+': '0'}")));
    }

    @Test
    public void testNonFlexibleVersionsAreNotCheckedAtParseTime() throws Exception {
        // The non-flexible side of the invariant is enforced against the generated code by
        // ApiMessageTypeTest, so parsing accepts a non-flexible version mapped to any header version.
        MessageSpec spec = parse(requestSpec("1-2", "1+", "{'0': '1', '1+': '2'}"));
        assertEquals(2, spec.headerVersions().orElseThrow().entries().size());
    }

    @Test
    public void testApiVersionsRequestFollowsFlexibilityRule() {
        // Only the response is pinned by KIP-511; the request follows the normal rule.
        assertMessageContains("which is flexible",
            () -> parse("{'apiKey': 18, 'type': 'request', 'name': 'ApiVersionsRequest', 'validVersions': '0-3', " +
                "'flexibleVersions': '3+', 'headerVersions': {'0+': '1'}}"));
    }
}
