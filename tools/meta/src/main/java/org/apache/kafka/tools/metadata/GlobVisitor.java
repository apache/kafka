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

package org.apache.kafka.tools.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Visits metadata paths based on a glob string.
 */
public final class GlobVisitor implements Consumer<MetadataNodeManager.Data> {
    private final String glob;
    private final Consumer<Optional<MetadataNodeInfo>> handler;

    public GlobVisitor(String glob,
                       Consumer<Optional<MetadataNodeInfo>> handler) {
        this.glob = glob;
        this.handler = handler;
    }

    public static class MetadataNodeInfo {
        private final String[] path;
        private final MetadataNode node;

        MetadataNodeInfo(String[] path, MetadataNode node) {
            this.path = path;
            this.node = node;
        }

        public String[] path() {
            return path;
        }

        public MetadataNode node() {
            return node;
        }

        public String absolutePath() {
            return "/" + String.join("/", path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, node);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MetadataNodeInfo)) return false;
            MetadataNodeInfo other = (MetadataNodeInfo) o;
            if (!Arrays.equals(path, other.path)) return false;
            if (!node.equals(other.node)) return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder("MetadataNodeInfo(path=");
            for (int i = 0; i < path.length; i++) {
                bld.append("/");
                bld.append(path[i]);
            }
            bld.append(", node=").append(node).append(")");
            return bld.toString();
        }
    }

    @Override
    public void accept(MetadataNodeManager.Data data) {
        if (glob.startsWith("/")) {
            accept(data.root(), glob);
        } else {
            accept(data.root(), data.workingDirectory() + "/" + glob);
        }
    }

    private void accept(MetadataNode.DirectoryNode root, String fullGlob) {
        List<String> globComponents = pathComponents(fullGlob);
        if (!accept(globComponents, 0, root, new String[0])) {
            handler.accept(Optional.empty());
        }
    }

    private boolean accept(List<String> globComponents,
                           int componentIndex,
                           MetadataNode node,
                           String[] path) {
        if (componentIndex >= globComponents.size()) {
            handler.accept(Optional.of(new MetadataNodeInfo(path, node)));
            return true;
        }
        String globComponentString = globComponents.get(componentIndex);
        if (globComponentString.equals(".")) {
            return accept(globComponents, componentIndex + 1, node, path);
        }
        if (globComponentString.equals("..")) {
            if (!(node instanceof MetadataNode.DirectoryNode)) {
                return false;
            }
            String[] newPath;
            if (path.length == 0) {
                newPath = path;
            } else {
                newPath = new String[path.length - 1];
                System.arraycopy(path, 0, newPath, 0, path.length - 1);
            }
            MetadataNode.DirectoryNode directory = (MetadataNode.DirectoryNode) node;
            return accept(globComponents, componentIndex + 1, directory.parent(), newPath);
        }
        GlobComponent globComponent = new GlobComponent(globComponentString);
        if (globComponent.literal()) {
            if (!(node instanceof MetadataNode.DirectoryNode)) {
                return false;
            }
            MetadataNode.DirectoryNode directory = (MetadataNode.DirectoryNode) node;
            MetadataNode child = directory.child(globComponent.component());
            if (child == null) {
                return false;
            }
            String[] newPath = new String[path.length + 1];
            System.arraycopy(path, 0, newPath, 0, path.length);
            newPath[path.length] = globComponent.component();
            return accept(globComponents, componentIndex + 1, child, newPath);
        }
        if (!(node instanceof MetadataNode.DirectoryNode)) {
            return false;
        }
        MetadataNode.DirectoryNode directory = (MetadataNode.DirectoryNode) node;
        boolean matchedAny = false;
        for (Entry<String, MetadataNode> entry : directory.children().entrySet()) {
            String nodeName = entry.getKey();
            if (globComponent.matches(nodeName)) {
                String[] newPath = new String[path.length + 1];
                System.arraycopy(path, 0, newPath, 0, path.length);
                newPath[path.length] = nodeName;
                if (accept(globComponents, componentIndex + 1, entry.getValue(), newPath)) {
                    matchedAny = true;
                }
            }
        }
        return matchedAny;
    }

    /**
     * Convert a path to a list of path components.
     * Multiple slashes in a row are treated the same as a single slash.
     * Trailing slashes are ignored.
     */
    public static List<String> pathComponents(String path) {
        List<String> results = new ArrayList<>();
        String[] components = path.split("/");
        for (int i = 0; i < components.length; i++) {
            if (!components[i].isEmpty()) {
                results.add(components[i]);
            }
        }
        return results;
    }
}