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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * A {@link Transformation} that translates UTF-16 surrogate pairs found in string fields
 * into one of three representations:
 * <ul>
 *   <li>{@code url-encode} (default) — percent-encodes the character using its UTF-8 byte sequence</li>
 *   <li>{@code java-encode} — encodes as {@code \UXXXX\UXXXX} Java-style surrogate pair notation</li>
 *   <li>{@code replace} — replaces the character with a configurable replacement string
 *       (defaults to the Unicode replacement character U+FFFD)</li>
 * </ul>
 * <p>
 * Use the concrete transformation type designed for the record key ({@link Key}) or value ({@link Value}).
 */
public abstract class TranslateSurrogates<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Translate UTF-16 surrogate pairs in string fields to their UTF-8 URL encoding, "
            + "a Java-style \\UXXXX notation, or a configurable replacement string."
            + "<p/>Applies to string fields at any depth in structs, maps, or arrays."
            + "<p/>Use the concrete transformation type designed for the record key (<code>"
            + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    public static final String FIELDS_CONFIG = "fields";
    public static final String MODE_CONFIG = "mode";
    public static final String REPLACEMENT_CONFIG = "replacement";

    static final String MODE_URL_ENCODE = "url-encode";
    static final String MODE_JAVA_ENCODE = "java-encode";
    static final String MODE_REPLACE = "replace";

    static final String UNICODE_REPLACEMENT_CHARACTER = "\uFFFD";

    private static final String PURPOSE = "translate surrogate pairs";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MODE_CONFIG,
                    ConfigDef.Type.STRING,
                    MODE_URL_ENCODE,
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> {
                            String mode = (String) value;
                            if (!MODE_URL_ENCODE.equals(mode)
                                    && !MODE_JAVA_ENCODE.equals(mode)
                                    && !MODE_REPLACE.equals(mode))
                                throw new ConfigException(name, value,
                                    "Must be one of: " + MODE_URL_ENCODE + ", "
                                        + MODE_JAVA_ENCODE + ", " + MODE_REPLACE);
                        },
                        () -> "one of: " + MODE_URL_ENCODE + ", " + MODE_JAVA_ENCODE + ", " + MODE_REPLACE),
                    ConfigDef.Importance.HIGH,
                    "Translation mode for surrogate pairs. Must be one of: "
                        + MODE_URL_ENCODE + " (default), " + MODE_JAVA_ENCODE + ", or " + MODE_REPLACE + ".")
            .define(FIELDS_CONFIG,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    ConfigDef.Importance.MEDIUM,
                    "Names of fields to translate. If empty, all string fields are scanned.")
            .define(REPLACEMENT_CONFIG,
                    ConfigDef.Type.STRING,
                    UNICODE_REPLACEMENT_CHARACTER,
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> {
                            String s = (String) value;
                            for (int i = 0; i < s.length(); ) {
                                int cp = s.codePointAt(i);
                                if (!Character.isBmpCodePoint(cp))
                                    throw new ConfigException(name, value,
                                        "Replacement string must not itself contain surrogate pairs.");
                                i += Character.charCount(cp);
                            }
                        },
                        () -> "a string without surrogate pairs"),
                    ConfigDef.Importance.LOW,
                    "Replacement string used when mode is '" + MODE_REPLACE + "'. Must not contain surrogate pairs.");

    // package-private for testing: maps a non-BMP code point to its encoded string representation
    Function<Integer, String> surrogateEncoder;

    private Set<String> fields;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        final String mode = config.getString(MODE_CONFIG);
        fields = new HashSet<>(config.getList(FIELDS_CONFIG));
        final String replacement = config.getString(REPLACEMENT_CONFIG);

        switch (mode) {
            case MODE_JAVA_ENCODE:
                surrogateEncoder = codePoint -> {
                    char[] chars = Character.toChars(codePoint);
                    StringBuilder sb = new StringBuilder();
                    for (char c : chars)
                        sb.append(String.format("\\U%04X", (int) c));
                    return sb.toString();
                };
                break;
            case MODE_REPLACE:
                surrogateEncoder = codePoint -> replacement;
                break;
            default: // url-encode
                surrogateEncoder = codePoint ->
                    URLEncoder.encode(new String(Character.toChars(codePoint)), StandardCharsets.UTF_8);
                break;
        }
    }

    @Override
    public R apply(R record) {
        final Object value = operatingValue(record);
        if (value == null)
            return record;
        return newRecord(record, translated(value));
    }

    private Object translated(Object input) {
        final Schema.Type type = ConnectSchema.schemaType(input.getClass());
        if (type == null)
            throw new DataException(TranslateSurrogates.class.getSimpleName()
                    + " cannot process value of type " + input.getClass().getName()
                    + " which is not supported by the Connect data API");

        switch (type) {
            case STRING:
                return translateString((String) input);
            case ARRAY: {
                @SuppressWarnings("unchecked")
                final List<Object> list = (List<Object>) input;
                final List<Object> result = new ArrayList<>(list.size());
                for (Object item : list)
                    result.add(item instanceof String ? translateString((String) item) : item);
                return result;
            }
            case MAP: {
                final Map<String, Object> map = requireMap(input, PURPOSE);
                final Map<String, Object> result = new HashMap<>(map);
                final Set<String> targetFields = fields.isEmpty() ? map.keySet() : fields;
                for (String field : targetFields) {
                    final Object v = map.get(field);
                    if (v instanceof String)
                        result.put(field, translateString((String) v));
                }
                return result;
            }
            case STRUCT: {
                final Struct struct = requireStruct(input, PURPOSE);
                final Struct result = new Struct(struct.schema());
                for (Field field : struct.schema().fields()) {
                    final Object orig = struct.get(field);
                    final boolean shouldTranslate = fields.isEmpty() || fields.contains(field.name());
                    result.put(field, shouldTranslate && orig instanceof String
                            ? translateString((String) orig)
                            : orig);
                }
                return result;
            }
            default:
                return input;
        }
    }

    // package-private for testing
    String translateString(String input) {
        final StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); ) {
            final int cp = input.codePointAt(i);
            if (!Character.isBmpCodePoint(cp)) {
                sb.append(surrogateEncoder.apply(cp));
                i += 2; // a surrogate pair is two chars
            } else {
                sb.appendCodePoint(cp);
                i++;
            }
        }
        return sb.toString();
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends TranslateSurrogates<R> {
        @Override
        protected Schema operatingSchema(R record) { return record.keySchema(); }

        @Override
        protected Object operatingValue(R record) { return record.key(); }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), updatedValue,
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends TranslateSurrogates<R> {
        @Override
        protected Schema operatingSchema(R record) { return record.valueSchema(); }

        @Override
        protected Object operatingValue(R record) { return record.value(); }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}
