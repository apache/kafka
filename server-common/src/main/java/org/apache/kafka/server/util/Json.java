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

package org.apache.kafka.server.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.apache.kafka.server.util.json.JsonValue;

import java.io.IOException;
import java.util.Optional;

/**
 * Provides methods for parsing JSON with Jackson and encoding to JSON with a simple and naive custom implementation.
 */
public final class Json {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parse a JSON string into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
     */
    public static Optional<JsonValue> parseFull(String input) {
        try {
            return Optional.ofNullable(tryParseFull(input));
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    /**
     * Parse a JSON string into a generic type T, or throw JsonProcessingException in the case of
     * exception.
     */
    public static <T> T parseStringAs(String input, Class<T> clazz) throws JsonProcessingException {
        return MAPPER.readValue(input, clazz);
    }

    /**
     * Parse a JSON byte array into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
     */
    public static Optional<JsonValue> parseBytes(byte[] input) throws IOException {
        try {
            return Optional.ofNullable(MAPPER.readTree(input)).map(JsonValue::apply);
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    public static JsonValue tryParseBytes(byte[] input) throws IOException {
        return JsonValue.apply(MAPPER.readTree(input));
    }

    /**
     * Parse a JSON byte array into a generic type T, or throws a JsonProcessingException in the case of exception.
     */
    public static <T> T parseBytesAs(byte[] input, Class<T> clazz) throws IOException {
        return MAPPER.readValue(input, clazz);
    }

    /**
     * Parse a JSON string into a JsonValue if possible.
     * @param input a JSON string to parse
     * @return the actual json value.
     * @throws JsonProcessingException if failed to parse
     */
    public static JsonValue tryParseFull(String input) throws JsonProcessingException {
        if (input == null || input.isEmpty()) {
            throw new JsonParseException(MissingNode.getInstance().traverse(), "The input string shouldn't be empty");
        } else {
            return JsonValue.apply(MAPPER.readTree(input));
        }
    }

    /**
     * Encode an object into a JSON string. This method accepts any type supported by Jackson's ObjectMapper in
     * the default configuration. That is, Java collections are supported, but Scala collections are not (to avoid
     * a jackson-scala dependency).
     */
    public static String encodeAsString(Object obj) throws JsonProcessingException {
        return MAPPER.writeValueAsString(obj);
    }

    /**
     * Encode an object into a JSON value in bytes. This method accepts any type supported by Jackson's ObjectMapper in
     * the default configuration. That is, Java collections are supported, but Scala collections are not (to avoid
     * a jackson-scala dependency).
     */
    public static byte[] encodeAsBytes(Object obj) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(obj);
    }
}
