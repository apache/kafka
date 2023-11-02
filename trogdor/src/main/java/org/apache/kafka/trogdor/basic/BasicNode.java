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

package org.apache.kafka.trogdor.basic;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.trogdor.common.Node;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BasicNode implements Node {
    private final String name;
    private final String hostname;
    private final Map<String, String> config;
    private final Set<String> tags;

    public BasicNode(String name, String hostname, Map<String, String> config,
                     Set<String> tags) {
        this.name = name;
        this.hostname = hostname;
        this.config = config;
        this.tags = tags;
    }

    public BasicNode(String name, JsonNode root) {
        this.name = name;
        String hostname = "localhost";
        Set<String> tags = Collections.emptySet();
        Map<String, String> config = new HashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> iter = root.fields();
             iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            JsonNode node = entry.getValue();
            if (key.equals("hostname")) {
                hostname = node.asText();
            } else if (key.equals("tags")) {
                if (!node.isArray()) {
                    throw new RuntimeException("Expected the 'tags' field to be an " +
                        "array of strings.");
                }
                tags = new HashSet<>();
                for (Iterator<JsonNode> tagIter = node.elements(); tagIter.hasNext(); ) {
                    JsonNode tag = tagIter.next();
                    tags.add(tag.asText());
                }
            } else {
                config.put(key, node.asText());
            }
        }
        this.hostname = hostname;
        this.tags = tags;
        this.config = config;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String hostname() {
        return hostname;
    }

    @Override
    public String getConfig(String key) {
        return config.get(key);
    }

    @Override
    public Set<String> tags() {
        return tags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, hostname, config, tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BasicNode that = (BasicNode) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(hostname, that.hostname) &&
            Objects.equals(config, that.config) &&
            Objects.equals(tags, that.tags);
    }
}
