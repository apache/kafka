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

package org.apache.kafka.shell;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A node in the metadata tool.
 */
public interface MetadataNode {
    class DirectoryNode implements MetadataNode {
        private final TreeMap<String, MetadataNode> children = new TreeMap<>();

        public DirectoryNode mkdirs(String... names) {
            if (names.length == 0) {
                throw new RuntimeException("Invalid zero-length path");
            }
            DirectoryNode node = this;
            for (int i = 0; i < names.length; i++) {
                MetadataNode nextNode = node.children.get(names[i]);
                if (nextNode == null) {
                    nextNode = new DirectoryNode();
                    node.children.put(names[i], nextNode);
                } else {
                    if (!(nextNode instanceof DirectoryNode)) {
                        throw new NotDirectoryException();
                    }
                }
                node = (DirectoryNode) nextNode;
            }
            return node;
        }

        public void rmrf(String... names) {
            if (names.length == 0) {
                throw new RuntimeException("Invalid zero-length path");
            }
            DirectoryNode node = this;
            for (int i = 0; i < names.length - 1; i++) {
                MetadataNode nextNode = node.children.get(names[i]);
                if (!(nextNode instanceof DirectoryNode)) {
                    throw new RuntimeException("Unable to locate directory /" +
                        String.join("/", names));
                }
                node = (DirectoryNode) nextNode;
            }
            node.children.remove(names[names.length - 1]);
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

        public MetadataNode child(String component) {
            return children.get(component);
        }

        public NavigableMap<String, MetadataNode> children() {
            return children;
        }

        public void addChild(String name, DirectoryNode child) {
            children.put(name, child);
        }

        public DirectoryNode directory(String... names) {
            if (names.length == 0) {
                throw new RuntimeException("Invalid zero-length path");
            }
            DirectoryNode node = this;
            for (int i = 0; i < names.length; i++) {
                MetadataNode nextNode = node.children.get(names[i]);
                if (!(nextNode instanceof DirectoryNode)) {
                    throw new RuntimeException("Unable to locate directory /" +
                        String.join("/", names));
                }
                node = (DirectoryNode) nextNode;
            }
            return node;
        }

        public FileNode file(String... names) {
            if (names.length == 0) {
                throw new RuntimeException("Invalid zero-length path");
            }
            DirectoryNode node = this;
            for (int i = 0; i < names.length - 1; i++) {
                MetadataNode nextNode = node.children.get(names[i]);
                if (!(nextNode instanceof DirectoryNode)) {
                    throw new RuntimeException("Unable to locate file /" +
                        String.join("/", names));
                }
                node = (DirectoryNode) nextNode;
            }
            MetadataNode nextNode = node.child(names[names.length -  1]);
            if (!(nextNode instanceof FileNode)) {
                throw new RuntimeException("Unable to locate file /" +
                    String.join("/", names));
            }
            return (FileNode) nextNode;
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
