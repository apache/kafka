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
import java.util.TreeSet;

public class ReadOnlyNode<T> implements NodeData<T> {
    public static <T> ReadOnlyNode<T> create(NodeData<T> data) {
        return data instanceof ReadOnlyNode ? (ReadOnlyNode<T>) data : new ReadOnlyNode<>(data);
    }

    NodeData<T> delegate;
    private ReadOnlyNode(NodeData<T> data) {
        this.delegate = data;
    }

    @Override
    public String getFragment() {
        return delegate.getFragment();
    }

    @Override
    public boolean hasContents() {
        return delegate.hasContents();
    }

    @Override
    public T getContents() {
        return delegate.getContents();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    public ReadOnlyNode<T> getParent() {
        NodeData<T> parent = delegate.getParent();
        return parent == null ? null : create(parent);
    }

    @Override
    public SortedSet<ReadOnlyNode<T>> getChildren() {
        TreeSet<ReadOnlyNode<T>> result = new TreeSet<>();
        // do not use stream as the overhead is too great on the hot path.
        delegate.getChildren().forEach(nodeData -> result.add(ReadOnlyNode.create(nodeData)));
        return result;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
