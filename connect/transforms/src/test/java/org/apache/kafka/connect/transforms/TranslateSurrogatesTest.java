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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TranslateSurrogatesTest {

    // U+23A3A (𣘺) — a non-BMP character represented as surrogate pair \uD84D\uDE3A
    private static final String SURROGATE_CHAR = "\uD84D\uDE3A";
    private static final int SURROGATE_CODE_POINT = 0x23A3A;

    private final TranslateSurrogates<SourceRecord> xformKey = new TranslateSurrogates.Key<>();
    private final TranslateSurrogates<SourceRecord> xformValue = new TranslateSurrogates.Value<>();

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    // -------------------------------------------------------------------------
    // Configuration validation
    // -------------------------------------------------------------------------

    @Test
    public void configRejectsEmptyMode() {
        assertThrows(ConfigException.class,
            () -> xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "")));
    }

    @Test
    public void configRejectsInvalidMode() {
        assertThrows(ConfigException.class,
            () -> xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "base64")));
    }

    @Test
    public void configRejectsReplacementContainingSurrogatePair() {
        assertThrows(ConfigException.class,
            () -> xformKey.configure(Collections.singletonMap(
                TranslateSurrogates.REPLACEMENT_CONFIG, SURROGATE_CHAR)));
    }

    // -------------------------------------------------------------------------
    // Encoder behaviour per mode
    // -------------------------------------------------------------------------

    @Test
    public void defaultModeUrlEncodesNonBmpCodePoint() {
        xformKey.configure(Collections.emptyMap());
        // U+23A3A → UTF-8 bytes F0 A3 98 BA → %F0%A3%98%BA
        assertEquals("%F0%A3%98%BA", xformKey.surrogateEncoder.apply(SURROGATE_CODE_POINT));
    }

    @Test
    public void javaEncodeModeEncodesSurrogatePair() {
        xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, TranslateSurrogates.MODE_JAVA_ENCODE));
        // Surrogate pair: \uD84D \uDE3A
        assertEquals("\\UD84D\\UDE3A", xformKey.surrogateEncoder.apply(SURROGATE_CODE_POINT));
    }

    @Test
    public void replaceModeUsesDefaultReplacementCharacter() {
        xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, TranslateSurrogates.MODE_REPLACE));
        assertEquals(TranslateSurrogates.UNICODE_REPLACEMENT_CHARACTER,
                xformKey.surrogateEncoder.apply(SURROGATE_CODE_POINT));
    }

    @Test
    public void replaceModeUsesConfiguredReplacementString() {
        Map<String, String> config = new HashMap<>();
        config.put(TranslateSurrogates.MODE_CONFIG, TranslateSurrogates.MODE_REPLACE);
        config.put(TranslateSurrogates.REPLACEMENT_CONFIG, "SURROGATE");
        xformKey.configure(config);
        assertEquals("SURROGATE", xformKey.surrogateEncoder.apply(SURROGATE_CODE_POINT));
    }

    // -------------------------------------------------------------------------
    // translateString — unit tests on the string translation method
    // -------------------------------------------------------------------------

    @Test
    public void translateStringLeavesPlainAsciiUnchanged() {
        xformValue.configure(Collections.emptyMap());
        assertEquals("hello world", xformValue.translateString("hello world"));
    }

    @Test
    public void translateStringUrlEncodesSurrogatePair() {
        xformValue.configure(Collections.emptyMap());
        assertEquals("abc%F0%A3%98%BAabc", xformValue.translateString("abc" + SURROGATE_CHAR + "abc"));
    }

    @Test
    public void translateStringJavaEncodesSurrogatePair() {
        xformValue.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, TranslateSurrogates.MODE_JAVA_ENCODE));
        assertEquals("abc\\UD84D\\UDE3Aabc", xformValue.translateString("abc" + SURROGATE_CHAR + "abc"));
    }

    @Test
    public void translateStringReplacesSurrogatePair() {
        Map<String, String> config = new HashMap<>();
        config.put(TranslateSurrogates.MODE_CONFIG, TranslateSurrogates.MODE_REPLACE);
        config.put(TranslateSurrogates.REPLACEMENT_CONFIG, "?");
        xformValue.configure(config);
        assertEquals("abc?abc", xformValue.translateString("abc" + SURROGATE_CHAR + "abc"));
    }

    @Test
    public void translateStringHandlesMultipleSurrogatePairs() {
        xformValue.configure(Collections.emptyMap());
        assertEquals("%F0%A3%98%BA%F0%A3%98%BA", xformValue.translateString(SURROGATE_CHAR + SURROGATE_CHAR));
    }

    // -------------------------------------------------------------------------
    // apply() on schemaless records
    // -------------------------------------------------------------------------

    @Test
    public void schemalessStringValueIsTranslated() {
        xformValue.configure(Collections.emptyMap());
        SourceRecord out = xformValue.apply(record(null, "abc" + SURROGATE_CHAR));
        assertEquals("abc%F0%A3%98%BA", out.value());
    }

    @Test
    public void schemalessMapAllFieldsTranslatedByDefault() {
        xformValue.configure(Collections.emptyMap());

        Map<String, Object> value = new HashMap<>();
        value.put("f1", 42);
        value.put("f2", "plain");
        value.put("f3", "abc" + SURROGATE_CHAR);
        value.put("f4", "abc" + SURROGATE_CHAR + "abc");

        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) xformValue.apply(record(null, value)).value();

        assertEquals(42, out.get("f1"));
        assertEquals("plain", out.get("f2"));
        assertEquals("abc%F0%A3%98%BA", out.get("f3"));
        assertEquals("abc%F0%A3%98%BAabc", out.get("f4"));
    }

    @Test
    public void schemalessMapOnlyConfiguredFieldsTranslated() {
        xformValue.configure(Collections.singletonMap(TranslateSurrogates.FIELDS_CONFIG, "f3"));

        Map<String, Object> value = new HashMap<>();
        value.put("f1", 42);
        value.put("f2", "plain");
        value.put("f3", "abc" + SURROGATE_CHAR);
        value.put("f4", "abc" + SURROGATE_CHAR + "abc");

        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) xformValue.apply(record(null, value)).value();

        assertEquals(42, out.get("f1"));
        assertEquals("plain", out.get("f2"));
        assertEquals("abc%F0%A3%98%BA", out.get("f3"));
        // f4 not in fields config — must be left unchanged
        assertEquals("abc" + SURROGATE_CHAR + "abc", out.get("f4"));
    }

    @Test
    public void schemalessArrayStringsAreTranslated() {
        xformValue.configure(Collections.emptyMap());

        List<Object> value = new ArrayList<>();
        value.add("plain");
        value.add("abc" + SURROGATE_CHAR);
        value.add("abc" + SURROGATE_CHAR + "abc");

        @SuppressWarnings("unchecked")
        List<Object> out = (List<Object>) xformValue.apply(record(null, value)).value();

        assertEquals("plain", out.get(0));
        assertEquals("abc%F0%A3%98%BA", out.get(1));
        assertEquals("abc%F0%A3%98%BAabc", out.get(2));
    }

    @Test
    public void nullValueIsPassedThrough() {
        xformValue.configure(Collections.emptyMap());
        SourceRecord in = record(null, null);
        assertEquals(in, xformValue.apply(in));
    }

    @Test
    public void unsupportedTypeThrowsDataException() {
        xformValue.configure(Collections.emptyMap());
        assertThrows(DataException.class,
            () -> xformValue.apply(record(null, new Date())));
    }

    // -------------------------------------------------------------------------
    // apply() on schema-based records
    // -------------------------------------------------------------------------

    @Test
    public void schemaBasedStructAllStringFieldsTranslated() {
        xformValue.configure(Collections.emptyMap());

        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("count", Schema.INT32_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("name", "abc" + SURROGATE_CHAR)
                .put("count", 7);

        Struct out = (Struct) xformValue.apply(record(schema, value)).value();
        assertEquals("abc%F0%A3%98%BA", out.get("name"));
        assertEquals(7, out.get("count"));
    }

    @Test
    public void schemaBasedStructOnlyConfiguredFieldsTranslated() {
        xformValue.configure(Collections.singletonMap(TranslateSurrogates.FIELDS_CONFIG, "f1"));

        Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("f1", "abc" + SURROGATE_CHAR)
                .put("f2", "abc" + SURROGATE_CHAR);

        Struct out = (Struct) xformValue.apply(record(schema, value)).value();
        assertEquals("abc%F0%A3%98%BA", out.get("f1"));
        // f2 not in fields config — must be left unchanged
        assertEquals("abc" + SURROGATE_CHAR, out.get("f2"));
    }

    @Test
    public void schemaBasedNestedStructIsTranslated() {
        xformValue.configure(Collections.emptyMap());

        Schema inner = SchemaBuilder.struct()
                .field("inner_f", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema outer = SchemaBuilder.struct()
                .field("outer_f", Schema.STRING_SCHEMA)
                .field("nested", inner)
                .build();

        Struct innerValue = new Struct(inner).put("inner_f", "abc" + SURROGATE_CHAR);
        Struct outerValue = new Struct(outer)
                .put("outer_f", "abc" + SURROGATE_CHAR)
                .put("nested", innerValue);

        Struct out = (Struct) xformValue.apply(record(outer, outerValue)).value();
        assertEquals("abc%F0%A3%98%BA", out.get("outer_f"));
        assertEquals("abc%F0%A3%98%BA", ((Struct) out.get("nested")).get("inner_f"));
    }

    // -------------------------------------------------------------------------
    // Key transform
    // -------------------------------------------------------------------------

    @Test
    public void keyTransformTranslatesKey() {
        xformKey.configure(Collections.emptyMap());
        SourceRecord out = xformKey.apply(record("abc" + SURROGATE_CHAR, "untouched"));
        assertEquals("abc%F0%A3%98%BA", out.key());
        assertEquals("untouched", out.value());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static SourceRecord record(Object key, Object value) {
        return new SourceRecord(null, null, "topic", 0, null, key, null, value);
    }
}
