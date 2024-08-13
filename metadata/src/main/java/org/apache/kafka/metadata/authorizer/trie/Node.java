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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.Stack;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The node definition for the Radix Trie.  There are 4 types of nodes.
 * <ul>
 *     <li>Root node - There is only one and it has no parent node.</li>
 *     <li>Leaf nodes - Has no child nodes and have "contents" set.</li>
 *     <li>Pure inner node - has at least one child and does <em>not<</em> have "contents" set.</li>
 *     <li>Inner node - has at least one child and has "contents" set.</li>
 * </ul>
 *
 * This implementation only uses Strings for names but any Object for which an Inserter and a Matcher can be build
 * may be implemented.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Radix_tree">Radix Tree (Wikipedia)</a>
 */
public class Node<T> implements NodeData<T> {
    /**
     * the parent node.  Will be {@code null} in the {@code root} and in {@code exemplar}s.
     */
    private Node<T> up;

    /**
     * the fragment of the text that this node represents.  Never {@code null}, will be an empty string
     * for the root node..
     */
    private final String fragment;

    /**
     * The children of this node.  May be {@code null}.
     */
    private SortedSet<Node<T>> children;

    /**
     * The contents of this node.  Will be {@code null} for root, exemplar, and pure inner nodes.
     */
    private T contents;

    /**
     * Lock to ensure that only one thread updates contents.
     */
    private final Lock contentLock;


    /**
     * Constructs a new root node for a new Trie
     */
    public static <T> Node<T> makeRoot() {
        return makeRoot("");
    }

    /**
     * Constructs a new root node for a new Trie
     */
    public static <T> Node<T> makeRoot(String fragment) {
        return new Node<>(null, fragment);
    }

    /**
     * Constructs the SortedSet of T for use as the children of this node.
     * @return
     * @param <T>
     */
    private synchronized SortedSet<Node<T>> createChildren() {
        if (children == null) {
            children = new ConcurrentSkipListSet<>();
        }
        return children;
    }


    /**
     * Constructor.
     *
     * @param parent   The parent of this node.  Is only {@code null} for the root.
     * @param fragment the fragment that this node adds to the key.  I empty for the root.
     */
    private Node(Node<T> parent, String fragment) {
        this.up = parent;
        this.fragment = fragment;
        // add this node to the parent if the parent is provided.
        if (up != null) {
            if (up.children == null) {
                up.createChildren();
            }
            up.children.add(this);
        }
        contentLock = new ReentrantLock();
    }

    /**
     * Sets the contents of this node.
     *
     * @param value The value for the node
     */
    void setContents(T value) {
        mergeContents(value, (a, b) -> value);
    }

    /**
     * Merge the contents of this node with the value specified using the remappingFunction.
     * <ul>
     *  <li>If the current node value is null the {@code value} parameter is used to set the node value.</li>
     *  <li>If the current node value is not null the current node value and the {@code value} parameter are
     *  passed to the {@code remappingFunction} in that order.</li>
     * @param value the desired value.
     * @param remappingFunction the remapping function to merge existing contents with the new value.
     */
    void mergeContents(T value, BiFunction<T, T, T> remappingFunction) {
        contentLock.lock();
        try {
            contents = contents == null ? value : remappingFunction.apply(contents, value);
        } finally {
            contentLock.unlock();
        }
    }

    void removeContents(Function<T, T> remappingFunction) {
        contentLock.lock();
        try {
            if (contents != null) {
                contents = remappingFunction.apply(contents);
            }
        } finally {
            contentLock.unlock();
        }
    }

    /**
     * A sorted set of children of this node.
     *
     * @return the sorted set of the child nodes of this node.
     */
    public SortedSet<Node<T>> getChildren() {
        return children == null ? Collections.emptySortedSet() : Collections.unmodifiableSortedSet(children);
    }

    Node<T> createChild(String fragment) {
        if (children == null) {
            children = createChildren();
        }
        Node<T> n = new Node<>(this, fragment);
        children.add(n);
        return n;
    }

    /**
     * Renames the node by creating a new node with the new parent node and fragment.
     * Copies the contents of this node to the new node.
     *
     * @param parent      the new parent node.
     * @param newFragment the new fragment.
     * @return the Now node with the proper parent and the parent having a reference ot this node.
     */
    Node<T> rename(Node<T> parent, String newFragment) {
        Node<T> newNode = new Node<T>(parent, newFragment);
        if (children != null) {
            newNode.children = createChildren();
            for (Node<T> child : children) {
                child.up = newNode;
                newNode.children.add(child);
            }
        }
        newNode.contents = contents;
        // do not call getChildren as we need to modify the set.
        this.getParent().children.remove(this);
        return newNode;
    }

    /**
     * Get the parent node.
     *
     * @return the parent of this node.  Will be {@code null} for the root and exemplar nodes.
     */
    public Node<T> getParent() {
        return up;
    }

    /**
     * Get the text fragment that this node contains.
     *
     * @return the text fragment.
     */
    @Override
    public String getFragment() {
        return fragment;
    }

    /**
     * Gets the object associated with this node.
     *
     * @return the object associated with this node.  May be {@code null}.
     */
    public T getContents() {
        return contents;
    }

    public boolean hasContents() {
        return contents != null;
    }

    /**
     * Gets the complete name of this node.
     *
     * @return the fully composed name of this node.
     */
    @Override
    public String getName() {
        StringBuilder sb = new StringBuilder();
        for (Node<T> n : pathTo()) {
            sb.append(n.fragment);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public int hashCode() {
        return fragment.hashCode();
    }

    /**
     * Nodes are equal if their fragments are equal and they have the same parent.
     * @param o the object to compare to.
     * @return true if the other object equals this node.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node<?> node = (Node<?>) o;
        return Objects.equals(fragment, node.fragment) && Objects.equals(up, node.up);
    }

    /**
     * Delete this node.
     * The contents of the node will be deleted.  The actual node will be deleted only if it is an
     * empty leaf node.
     */
    void delete() {
        // if up is null we are the root.  Can not delete the root.
        if (up != null) {
            if (contents != null) {
                contents = null;
            }
            // only remove empty leaf nodes.
            if (children == null || children.isEmpty()) {
                up.children.remove(this);
                // if removal of empty leaf node creates new empty leaf node
                // remove it.
                if (up.children.isEmpty() && up.contents == null) {
                    up.delete();
                }
            }
        }
    }

    /**
     * Build the list of nodes from the root to this node excluding the root and including this node.
     *
     * @return the list of nodes from the root in order.
     */
    public List<Node<T>> pathTo() {
        Stack<Node<T>> stack = new Stack<>();
        Node<T> working = this;
        while (working.up != null) {
            stack.push(working);
            working = working.up;
        }
        List<Node<T>> result = new ArrayList<>();
        while (!stack.isEmpty()) {
            result.add(stack.pop());
        }
        return result;
    }
}
