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
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JsonObject implements JsonValue {
    protected final ObjectNode node;

    JsonObject(ObjectNode node) {
        this.node = node;
    }

    @Override
    public JsonNode node() {
        return node;
    }

    public JsonValue apply(String name) throws JsonMappingException {
        return get(name).orElseThrow(() -> new JsonMappingException(null, "No such field exists: `" + name + "`"));
    }

    public Optional<JsonValue> get(String name) {
        return Optional.ofNullable(node().get(name)).map(JsonValue::apply);
    }

    public Iterator<Map.Entry<String, JsonValue>> iterator() {
        Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
        Stream<Map.Entry<String, JsonNode>> stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);

        Stream<Map.Entry<String, JsonValue>> results = stream.map(entry ->
                new AbstractMap.SimpleEntry<>(entry.getKey(), JsonValue.apply(entry.getValue()))
        );
        return results.toList().iterator();
    }

    @Override
    public int hashCode() {
        return node().hashCode();
    }

    @Override
    public boolean equals(Object a) {
        if (a instanceof JsonObject) {
            return node().equals(((JsonObject) a).node());
        }
        return false;
    }

    @Override
    public String toString() {
        return node().toString();
    }
}
