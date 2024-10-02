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
package org.apache.kafka.metadata.authorizer.trie;

/**
 * An interface that defines a Traverser.  A traverser starts at the root of the tree and moves
 * thorugh the nodes in a programmatically defined order until a specific condition is met.  It is intended
 * that the implementing class will retain data that is necessary for continued processing.
 * @param <T> the data type for the Nodes.
 */
public interface Traverser<T> {
    /**
     * Traverse the tree starting at the root.
     * @param root The root node of the tree.
     */
    void traverse(Node<T> root);
}
