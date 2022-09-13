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

package org.apache.kafka.metadata.authorizer;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.function.Function;


/**
 * A node in the tree which handles prefix ACLs. We also handle wildcard ACLs here by treating them
 * like prefix ACLs with a prefix of the empty string.
 *
 * The root node's name is the empty string. Each node contains zero or more children, all of
 * whom have names which are suffixes of their parent's name. We can visit all of the PrefixNode
 * objects for a given string in logarithmic time simply by walking down the tree.
 *
 * The prefix tree is immutable. New instances are constructed by PrefixTreeBuilder objects out of
 * a combination of new and old parts, as appropriate.
 */
final class PrefixNode {
    static final PrefixNode EMPTY =
            new PrefixNode(Collections.emptyNavigableMap(), "", ResourceAcls.EMPTY);
    private final NavigableMap<String, PrefixNode> children;
    private final String name;
    private final ResourceAcls resourceAcls;

    PrefixNode(
        NavigableMap<String, PrefixNode> children,
        String name,
        ResourceAcls resourceAcls
    ) {
        this.children = children;
        this.name = name;
        this.resourceAcls = resourceAcls;
    }

    NavigableMap<String, PrefixNode> children() {
        return children;
    }

    String name() {
        return name;
    }

    boolean isRoot() {
        return name.isEmpty();
    }

    ResourceAcls resourceAcls() {
        return resourceAcls;
    }

    /**
     * Walk the tree along a path dictated by the provided name. This function must be called
     * from the root node.
     *
     * @param name              The resource name to view nodes for.
     * @param visitor           The visitor object.
     */
    void walk(
        String name,
        Function<PrefixNode, Boolean> visitor
    ) {
        if (!visitor.apply(this)) return;
        Map.Entry<String, PrefixNode> entry = children.floorEntry(name);
        if (entry == null) return;
        String childPrefix = entry.getKey();
        if (name.startsWith(childPrefix)) {
            PrefixNode child = entry.getValue();
            child.walk(name, visitor);
        }
    }

    /**
     * Walk the tree, potentially following all paths. This function must be called from the
     * root node.
     *
     * @param visitor           The visitor object.
     */
    boolean walk(
        Function<PrefixNode, Boolean> visitor
    ) {
        if (!visitor.apply(this)) return false;
        for (Map.Entry<String, PrefixNode> entry : children.entrySet()) {
            if (!entry.getValue().walk(visitor)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(children, name, resourceAcls);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        PrefixNode other = (PrefixNode) o;
        return name.equals(other.name) &&
            resourceAcls.equals(other.resourceAcls) &&
            children.equals(other.children);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PrefixNode");
        builder.append("(name=").append(name);
        builder.append(", resourceAcls=").append(resourceAcls);
        builder.append(", children=(");
        String prefix = "";
        for (Map.Entry<String, PrefixNode> entry : children.entrySet()) {
            builder.append(prefix).append("Entry(name=").append(entry.getKey());
            builder.append(", value=").append(entry.getValue()).append(")");
            prefix = ",";
        }
        builder.append(")");
        builder.append(")");
        return builder.toString();
    }
}
