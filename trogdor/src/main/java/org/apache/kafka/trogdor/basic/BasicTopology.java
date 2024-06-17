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
import org.apache.kafka.trogdor.common.Topology;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class BasicTopology implements Topology {
    private final NavigableMap<String, Node> nodes;

    public BasicTopology(NavigableMap<String, Node> nodes) {
        this.nodes = nodes;
    }

    public BasicTopology(JsonNode configRoot) {
        if (!configRoot.isObject()) {
            throw new RuntimeException("Expected the 'nodes' element to be " +
                "a JSON object.");
        }
        nodes = new TreeMap<>();
        for (Iterator<String> iter = configRoot.fieldNames(); iter.hasNext(); ) {
            String nodeName = iter.next();
            JsonNode nodeConfig = configRoot.get(nodeName);
            BasicNode node = new BasicNode(nodeName, nodeConfig);
            nodes.put(nodeName, node);
        }
    }

    @Override
    public Node node(String id) {
        return nodes.get(id);
    }

    @Override
    public NavigableMap<String, Node> nodes() {
        return nodes;
    }
}
