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

import java.util.SortedSet;

/**
 * The data stored on a node.
 * @param <T> the Structure of the contents.
 */
public interface NodeData<T> extends FragmentHolder {
    /**
     * Returns {@code true} if the node has contents.
     * @return {@code true} if the node has contents.
     */
    boolean hasContents();

    /**
     * Returns the contents of the node.
     * @return the contents of the node, may be {@code null}.
     */
    T getContents();

    /**
     * Gets the name of the node.  This is the fully qualified name from the root to this node.
     * @return the fully qualified name of this node.
     */
    String getName();

    /**
     * Returns the parent of this node.  Will be {@code null} if this is the root.
     * @return the parent node.
     */
    NodeData<T> getParent();

    /**
     * Returns the children of this node.  Will return an empty set if there are no children.
     * @return the children of this node or an empty set.
     */
    SortedSet<? extends NodeData<T>> getChildren();
}
