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

package kafka.testkit;

import org.apache.kafka.common.Uuid;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TestKitNodes {
    public static class Builder {
        private Uuid clusterId = null;
        private final NavigableMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();

        public Builder setClusterId(Uuid clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder addNodes(TestKitNode[] nodes) {
            for (TestKitNode node : nodes) {
                addNode(node);
            }
            return this;
        }

        public Builder addNode(TestKitNode node) {
            if (node instanceof ControllerNode) {
                ControllerNode controllerNode = (ControllerNode) node;
                controllerNodes.put(node.id(), controllerNode);
            } else {
                throw new RuntimeException("Can't handle TestKitNode subclass " +
                        node.getClass().getSimpleName());
            }
            return this;
        }

        public Builder setNumControllerNodes(int numControllerNodes) {
            if (numControllerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numControllerNodes");
            }
            while (controllerNodes.size() > numControllerNodes) {
                Iterator<Map.Entry<Integer, ControllerNode>> iter =
                    controllerNodes.entrySet().iterator();
                iter.next();
                iter.remove();
            }
            while (controllerNodes.size() < numControllerNodes) {
                int nextId = 3000;
                if (!controllerNodes.isEmpty()) {
                    nextId = controllerNodes.lastKey() + 1;
                }
                controllerNodes.put(nextId, new ControllerNode.Builder().
                    setId(nextId).build());
            }
            return this;
        }

        public TestKitNodes build() {
            if (clusterId == null) {
                clusterId = Uuid.randomUuid();
            }
            return new TestKitNodes(clusterId, controllerNodes);
        }
    }

    private final Uuid clusterId;
    private final NavigableMap<Integer, ControllerNode> controllerNodes;

    private TestKitNodes(Uuid clusterId, NavigableMap<Integer, ControllerNode> controllerNodes) {
        this.clusterId = clusterId;
        this.controllerNodes = controllerNodes;
    }

    public Uuid clusterId() {
        return clusterId;
    }

    public Map<Integer, ControllerNode> controllerNodes() {
        return controllerNodes;
    }
}
