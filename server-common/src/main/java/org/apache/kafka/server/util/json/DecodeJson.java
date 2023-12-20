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

package org.apache.kafka.server.util.json;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DecodeJson<T> {
    /**
     * Decode the JSON node provided into an instance of `T`.
     *
     * @throws JsonMappingException if `node` cannot be decoded into `T`.
     */
    T decode(JsonNode node) throws JsonMappingException;

    static JsonMappingException throwJsonMappingException(String expectedType, JsonNode node) {
        return new JsonMappingException(null, String.format("Expected `%s` value, received %s", expectedType, node));
    }

    final class DecodeBoolean implements DecodeJson<Boolean> {
        @Override
        public Boolean decode(JsonNode node) throws JsonMappingException {
            if (node.isBoolean()) {
                return node.booleanValue();
            }
            throw throwJsonMappingException(Boolean.class.getSimpleName(), node);
        }
    }

    final class DecodeDouble implements DecodeJson<Double> {
        @Override
        public Double decode(JsonNode node) throws JsonMappingException {
            if (node.isDouble() || node.isLong() || node.isInt()) {
                return node.doubleValue();
            }
            throw throwJsonMappingException(Double.class.getSimpleName(), node);
        }
    }

    final class DecodeInteger implements DecodeJson<Integer> {
        @Override
        public Integer decode(JsonNode node) throws JsonMappingException {
            if (node.isInt()) {
                return node.intValue();
            }
            throw throwJsonMappingException(Integer.class.getSimpleName(), node);
        }
    }

    final class DecodeLong implements DecodeJson<Long> {
        @Override
        public Long decode(JsonNode node) throws JsonMappingException {
            if (node.isLong() || node.isInt()) {
                return node.longValue();
            }
            throw throwJsonMappingException(Long.class.getSimpleName(), node);
        }
    }

    final class DecodeString implements DecodeJson<String> {
        @Override
        public String decode(JsonNode node) throws JsonMappingException {
            if (node.isTextual()) {
                return node.textValue();
            }
            throw throwJsonMappingException(String.class.getSimpleName(), node);
        }
    }

    static <E> DecodeJson<Optional<E>> decodeOptional(DecodeJson<E> decodeJson) {
        return node -> {
            if (node.isNull()) return Optional.empty();
            return Optional.of(decodeJson.decode(node));
        };
    }

    static <E> DecodeJson<List<E>> decodeList(DecodeJson<E> decodeJson) {
        return node -> {
            if (node.isArray()) {
                List<E> result = new ArrayList<>();
                Iterator<JsonNode> elements = node.elements();
                while (elements.hasNext()) {
                    result.add(decodeJson.decode(elements.next()));
                }
                return result;
            }
            throw throwJsonMappingException("JSON array", node);
        };
    }

    static <V> DecodeJson<Map<String, V>> decodeMap(DecodeJson<V> decodeJson) {
        return node -> {
            if (node.isObject()) {
                Map<String, V> result = new HashMap<>();
                Iterator<Map.Entry<String, JsonNode>> elements = node.fields();
                while (elements.hasNext()) {
                    Map.Entry<String, JsonNode> next = elements.next();
                    result.put(next.getKey(), decodeJson.decode(next.getValue()));
                }
                return result;
            }
            throw throwJsonMappingException("JSON object", node);
        };
    }
}
