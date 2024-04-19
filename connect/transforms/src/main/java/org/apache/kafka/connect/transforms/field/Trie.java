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
package org.apache.kafka.connect.transforms.field;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Data structure to organize overlapping {@code SingleFieldPath}s into a {@code MultiFieldPaths}.
 * A trie flattens overlapping paths (e.g. `foo` and `foo.bar` in V2, only `foo.bar` would be kept)
 */
class Trie {

    final Node root;

    Trie() {
        root = new Node();
    }

    void insert(SingleFieldPath path) {
        Node current = root;

        SingleFieldPath p = null;
        for (String step : path.stepsWithoutLast()) {
            p = p == null
                ? new SingleFieldPath(step, path.fieldSyntaxVersion())
                : p.append(step);
            current = current.addStep(step, p);
        }

        final String step = path.lastStep();
        current.addStep(step, path);
    }

    boolean isEmpty() {
        return root.isLeaf();
    }

    Node get(String step) {
        return root.get(step);
    }

    int size() {
        if (root.isLeaf()) return 0;
        return root.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trie trie = (Trie) o;
        return Objects.equals(root, trie.root);
    }

    @Override
    public int hashCode() {
        return Objects.hash(root);
    }

    @Override
    public String toString() {
        return "Trie(" +
            "root = " + root +
            ')';
    }

    static class Node {
        final Map<String, Node> steps = new HashMap<>();
        SingleFieldPath path;

        private Node() {
        }

        private Node(SingleFieldPath path) {
            this.path = path;
        }

        private Node addStep(String step, SingleFieldPath path) {
            return steps.computeIfAbsent(step, ignored -> {
//                if (this.path != null) this.path = null;
                return new Node(path);
            });
        }

        Node get(String step) {
            return steps.get(step);
        }

        public boolean isLeaf() {
            return steps.isEmpty();
        }

        Map<String, Node> steps() {
            return new HashMap<>(steps);
        }

        int size() {
            if (isLeaf()) return 1;
            int size = 0;
            for (Node child : steps.values()) {
                size = size + child.size();
            }
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node trieNode = (Node) o;
            return Objects.equals(steps, trieNode.steps) && Objects.equals(path, trieNode.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(steps, path);
        }

        @Override
        public String toString() {
            return "TrieNode(" +
                "steps = " + steps +
                (path != null ? (", path = " + path) : "") +
                ')';
        }
    }
}
