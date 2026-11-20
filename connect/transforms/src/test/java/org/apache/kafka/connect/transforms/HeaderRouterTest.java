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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HeaderRouterTest {

    private final HeaderRouter<SourceRecord> router = new HeaderRouter<>();

    @AfterEach
    public void tearDown() {
        router.close();
    }

    private SourceRecord sourceRecord(String topic, ConnectHeaders headers) {
        return new SourceRecord(Map.of("p", "v"), Map.of("o", "v"),
                topic, 0, null, "key", null, "value", 0L, headers);
    }

    @Test
    public void singleHeaderMatch() {
        router.configure(Map.of("header.names", "x-dest-topic"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("x-dest-topic", "target-topic");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("target-topic", result.topic());
    }

    @Test
    public void priorityOrderingFirstHeaderWins() {
        router.configure(Map.of("header.names", "tenant-id, classification"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("tenant-id", "acme");
        headers.addString("classification", "sensitive");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("acme", result.topic());
    }

    @Test
    public void priorityOrderingFallsToSecondHeader() {
        router.configure(Map.of("header.names", "tenant-id, classification"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("classification", "sensitive");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("sensitive", result.topic());
    }

    @Test
    public void fallbackTopicUsedWhenNoHeaderMatches() {
        router.configure(Map.of("header.names", "x-dest-topic", "fallback.topic", "default-topic"));
        SourceRecord result = router.apply(sourceRecord("source-topic", new ConnectHeaders()));
        assertEquals("default-topic", result.topic());
    }

    @Test
    public void passThroughWhenNoHeaderMatchesAndNoFallback() {
        router.configure(Map.of("header.names", "x-dest-topic"));
        SourceRecord result = router.apply(sourceRecord("source-topic", new ConnectHeaders()));
        assertEquals("source-topic", result.topic());
    }

    @Test
    public void nullHeaderValueIsSkipped() {
        router.configure(Map.of("header.names", "first, second"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.add("first", null, null);
        headers.addString("second", "second-topic");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("second-topic", result.topic());
    }

    @Test
    public void bytesHeaderValueDecodedAsUtf8() {
        router.configure(Map.of("header.names", "x-dest-topic"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("x-dest-topic", "bytes-topic".getBytes(StandardCharsets.UTF_8));
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("bytes-topic", result.topic());
    }

    @Test
    public void stringHeaderValueUsedDirectly() {
        router.configure(Map.of("header.names", "x-dest-topic"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("x-dest-topic", "string-topic");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("string-topic", result.topic());
    }

    @Test
    public void lastHeaderWithNameIsUsedWhenDuplicatesExist() {
        router.configure(Map.of("header.names", "x-dest-topic"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("x-dest-topic", "first-value");
        headers.addString("x-dest-topic", "last-value");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("last-value", result.topic());
    }

    @Test
    public void emptyStringHeaderValueIsSkipped() {
        router.configure(Map.of("header.names", "first, second"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("first", "");
        headers.addString("second", "second-topic");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("second-topic", result.topic());
    }

    @Test
    public void blankFallbackTopicTreatedAsPassThrough() {
        router.configure(Map.of("header.names", "x-dest-topic", "fallback.topic", "   "));
        SourceRecord result = router.apply(sourceRecord("source-topic", new ConnectHeaders()));
        assertEquals("source-topic", result.topic());
    }

    @Test
    public void missingRequiredHeaderNamesConfigThrows() {
        assertThrows(ConfigException.class, () -> router.configure(Map.of()));
    }

    @Test
    public void emptyHeaderNamesListThrows() {
        assertThrows(ConfigException.class, () -> router.configure(Map.of("header.names", "")));
    }

    @Test
    public void headersArePreservedAfterRerouting() {
        router.configure(Map.of("header.names", "x-dest-topic"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("x-dest-topic", "target-topic");
        headers.addString("x-keep", "preserved");
        SourceRecord result = router.apply(sourceRecord("source-topic", headers));
        assertEquals("target-topic", result.topic());
        assertEquals(headers, result.headers());
    }

    @Test
    public void versionMatchesAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), router.version());
    }
}
