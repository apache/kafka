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

package org.apache.kafka.castle.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Iterator;
import java.util.Map;

public class JsonTransformer {
    /**
     * Retrieves the value of a given key.
     */
    public interface Substituter {
        /**
         * Figure out what value to substitute into the JSON for the given key.
         *
         * @param key   The key.
         * @return      null if no substitution should be made; the text, otherwise.
         */
        String substitute(String key);
    }

    public static class MapSubstituter implements Substituter {
        private final Map<String, String> map;

        public MapSubstituter(Map<String, String> map) {
            this.map = map;
        }

        @Override
        public String substitute(String key) {
            return map.get(key);
        }
    }

    /**
     * Given a JsonNode, return a deep copy of that JSON node.  All of the String values
     * will be transformed using the provided transforms map.
     *
     * @param input         The JsonNode to copy.  Will not be modified.
     * @param substituter   The substitutor to use.
     * @return              A deep copy of the input node.
     */
    public static JsonNode transform(JsonNode input, Substituter substituter) {
        switch (input.getNodeType()) {
            case STRING:
                return TextNode.valueOf(transformString(input.textValue(), substituter));
            case ARRAY:
                ArrayNode arrayResult = new ArrayNode(JsonNodeFactory.instance);
                int index = 0;
                for (Iterator<JsonNode> iter = input.elements(); iter.hasNext(); ) {
                    JsonNode child = iter.next();
                    arrayResult.insert(index, transform(child, substituter));
                    index++;
                }
                return arrayResult;
            case OBJECT:
                ObjectNode objectResult = new ObjectNode(JsonNodeFactory.instance);
                for (Iterator<Map.Entry<String, JsonNode>> iter = input.fields(); iter.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = iter.next();
                    objectResult.set(transformString(entry.getKey(), substituter),
                        transform(entry.getValue(), substituter));
                }
                return objectResult;
            default:
                return input.deepCopy();
        }
    }

    /**
     * Transforms a string by replacing all strings of the form %{foo} with the value of
     * the "foo" key in the "transforms" map.  Note that the brackets are required-- we
     * will not replace %foo.  If there is no value for "foo" in the transforms map,
     * the empty string is used.  Backslashes can be used to escape characters; a double
     * backslash can be used to insert a literal backslash.
     *
     * @param input         The input string.
     * @param substituter   The substitutor to use.
     * @return              The transformed string.
     */
    public static String transformString(String input, Substituter substituter) {
        StringBuilder output = new StringBuilder();
        char delimiter = '\0';
        String variableName = "";
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (delimiter) {
                case '\\':
                    output.append(c);
                    delimiter = '\0';
                    break;
                case '%':
                    if (c != '{') {
                        output.append('%').append(c);
                        delimiter = '\0';
                    } else {
                        delimiter = '{';
                    }
                    break;
                case '{':
                    if (c == '}') {
                        String value = substituter.substitute(variableName);
                        if (value == null) {
                            output.append("%{" + variableName + "}");
                        } else {
                            output.append(value);
                        }
                        variableName = "";
                        delimiter = '\0';
                    } else {
                        variableName = variableName + c;
                    }
                    break;
                default:
                    switch (c) {
                        case '\\':
                            delimiter = '\\';
                            break;
                        case '%':
                            delimiter = '%';
                            break;
                        default:
                            output.append(c);
                            break;
                    }
                    break;
            }
        }
        return output.toString();
    }
};
