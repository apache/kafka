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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JsonArray implements JsonValue {
    protected final ArrayNode node;

    JsonArray(ArrayNode node) {
        this.node = node;
    }

    @Override
    public JsonNode node() {
        return node;
    }

    public Iterator<JsonValue> iterator() {
        Stream<JsonNode> nodeStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(node.elements(), Spliterator.ORDERED),
                false);
        Stream<JsonValue> results = nodeStream.map(JsonValue::apply);
        return results.collect(Collectors.toList()).iterator();
    }

    @Override
    public int hashCode() {
        return node().hashCode();
    }

    @Override
    public boolean equals(Object a) {
        if (a instanceof JsonArray) {
            return node().equals(((JsonArray) a).node());
        }
        return false;
    }

    @Override
    public String toString() {
        return node().toString();
    }
}
