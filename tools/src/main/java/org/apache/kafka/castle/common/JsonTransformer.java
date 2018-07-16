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
     * Given a JsonNode, return a deep copy of that JSON node.  All of the String values
     * will be transformed using the provided transforms map.
     *
     * @param input         The JsonNode to copy.  Will not be modified.
     * @param transforms    The transforms.
     * @return              A deep copy of the input node.
     */
    public static JsonNode transform(JsonNode input, Map<String, String> transforms) {
        switch (input.getNodeType()) {
            case STRING:
                return TextNode.valueOf(transformString(input.textValue(), transforms));
            case ARRAY:
                ArrayNode arrayResult = new ArrayNode(JsonNodeFactory.instance);
                int index = 0;
                for (Iterator<JsonNode> iter = input.elements(); iter.hasNext(); ) {
                    JsonNode child = iter.next();
                    arrayResult.insert(index, transform(child, transforms));
                    index++;
                }
                return arrayResult;
            case OBJECT:
                ObjectNode objectResult = new ObjectNode(JsonNodeFactory.instance);
                for (Iterator<Map.Entry<String, JsonNode>> iter = input.fields(); iter.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = iter.next();
                    objectResult.set(entry.getKey(), transform(entry.getValue(), transforms));
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
     * @param transforms    The transforms map.
     * @return              The transformed string.
     */
    public static String transformString(String input, Map<String, String> transforms) {
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
                        String value = transforms.get(variableName);
                        variableName = "";
                        if (value == null) {
                            value = "";
                        }
                        output.append(value);
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
