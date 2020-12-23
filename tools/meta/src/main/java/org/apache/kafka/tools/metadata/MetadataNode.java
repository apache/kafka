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

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A node in the metadata tool.
 */
public interface MetadataNode {
    class DirectoryNode implements MetadataNode {
        private final DirectoryNode parent;
        private final TreeMap<String, MetadataNode> children = new TreeMap<>();

        public DirectoryNode(DirectoryNode parent) {
            this.parent = parent == null ? this : parent;
        }

        public DirectoryNode mkdirs(String name) {
            MetadataNode node = children.get(name);
            if (node == null) {
                node = new DirectoryNode(this);
                children.put(name, node);
            } else {
                if (!(node instanceof DirectoryNode)) {
                    throw new NotDirectoryException();
                }
            }
            return (DirectoryNode) node;
        }

        public FileNode create(String name) {
            MetadataNode node = children.get(name);
            if (node == null) {
                node = new FileNode();
                children.put(name, node);
            } else {
                if (!(node instanceof FileNode)) {
                    throw new NotFileException();
                }
            }
            return (FileNode) node;
        }

        public DirectoryNode parent() {
            return parent;
        }

        public MetadataNode child(String component) {
            return children.get(component);
        }

        public NavigableMap<String, MetadataNode> children() {
            return children;
        }
    }

    class FileNode implements MetadataNode {
        private String contents;

        void setContents(String contents) {
            this.contents = contents;
        }

        String contents() {
            return contents;
        }
    }
}