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
import java.util.TreeSet;
import java.util.function.IntConsumer;

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
public class Node<T> implements FragmentHolder<String> {
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
     * Constructs a new root node for a new Trie
     */
    public static <T> Node<T> makeRoot() {
        return new Node<>(null, "");
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
                up.children = new TreeSet<>();
            }
            up.children.add(this);
        }
    }

    /**
     * Sets the contents of this node.
     *
     * @param value The value for the node
     * @return the old value for the node if any.
     */
    T setContents(T value) {
        T result = contents;
        contents = value;
        return result;
    }

    /**
     * A sorted set of children of this node.
     *
     * @return the sorted set of the child nodes of this node.
     */
    SortedSet<Node<T>> getChildren() {
        return children == null ? Collections.emptySortedSet() : Collections.unmodifiableSortedSet(children);
    }

    /**
     * Renames the node by creating a new node with the new parent node and fragment.
     * Copies the contents of this node to the new node.
     *
     * @param parent      the new parent node.
     * @param newFragment the new fragment.
     * @return the Now node with the proper parent and the parent having a reference ot this node.
     */
    private Node<T> rename(Node<T> parent, String newFragment) {
        Node<T> newNode = new Node<T>(parent, newFragment);
        if (children != null) {
            newNode.children = new TreeSet<>();
            for (Node<T> child : children) {
                child.up = newNode;
                newNode.children.add(child);
            }
        }
        newNode.contents = contents;
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

    /**
     * Gets the complete name of this node.
     *
     * @return the fully composed name of this node.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Node<T> n : pathTo()) {
            sb.append(n.fragment);
        }
        return sb.toString();
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
     * Adds a node for the value.
     * <ol>
     *     <li>The node may have already existed in which case the located node is returned.</li>
     *     <li>The node may be added at any level in the Trie structure.</li>
     * </ol>
     *
     * This is a recursive method.
     *
     * @param inserter identifies the node to locate.
     * @return the added or found Node.
     */
    Node<T> addNodeFor(StringInserter inserter) {
        // If the inserter is empty then we have found the Node.
        if (inserter.isEmpty()) {
            return this;
        }

        // if the inserter is on a wildcard use wildcard only.
        if (inserter.isWildcard()) {
            // search the children
            Node<T> test = null;
            if (children == null) {
                children = new TreeSet<>();
            } else {
                test = new Search().eq(inserter);
            }
            // if there is no matching node then create it using this fragment.
            if (test == null) {
                test = new Node<>(this, inserter.getFragment());
            }
            // recurse adding the next fragment to the found node
            return test.addNodeFor(inserter.advance(1));
        }

        // process non-wildcard segments
        String segment = inserter.getFragment();
        if (children != null) {
            for (Node<T> child : children) {
                // skip wildcard children
                if (!WildcardRegistry.isWildcard(child.fragment)) {
                    // segment extends or is equal to child fragment so add child to child.
                    if (segment.startsWith(child.fragment)) {
                        return child.addNodeFor(inserter.advance(child.fragment.length()));
                    }

                    // child extends segment; insert segment as new child and original child as segments's child.
                    //     A                              A
                    //   +----+                         +----+
                    //  BCD   CB   insert "AC" yields  BCD   C
                    //                                       B
                    //
                    if (child.fragment.startsWith(segment)) {
                        Node<T> newNode = new Node<>(this, segment);
                        // adds renamed child to newNode.children
                        child.rename(newNode, child.fragment.substring(segment.length()));
                        children.remove(child);
                        return newNode.addNodeFor(inserter.advance(segment.length()));
                    }

                    // check partial match case
                    //  ABCD    insert ACAB  yields  A
                    //                             +----+
                    //                            BCD  CAB
                    //
                    int limit = Math.min(child.fragment.length(), segment.length());
                    for (int i = 0; i < limit; i++) {
                        if (child.fragment.charAt(i) != segment.charAt(i)) {
                            if (i == 0) {
                                break;
                            }
                            // newNode adds inserted children
                            Node<T> newNode = new Node<>(this, child.fragment.substring(0, i));
                            // rename adds old child to newNode.children
                            child.rename(newNode, child.fragment.substring(i));
                            // newChild contains the remainder of the fragment and is child of newNode,
                            Node<T> newChild = new Node<>(newNode, segment.substring(i));
                            // remove the child we are replacing.
                            children.remove(child);
                            return newChild.addNodeFor(inserter.advance(segment.length()));
                        }
                    }
                }
            }
        }
        // no children; create child node of this node with the segment, and continue insert.
        return new Node<T>(this, segment).addNodeFor(inserter.advance(segment.length()));
    }

    /**
     * Find a Node based on a Matcher.
     *
     * @param matcher the matcher that determines the node to find.
     * @return The Node on which the find stopped, will be the "root" node if no match is found.
     */
    Node<T> findNodeFor(Matcher<T> matcher) {
        // this node is a match return.
        if (matcher.test(this)) {
            return this;
        }

        Search searcher = new Search();
        if (children != null) {
            // find exact(ish) match first.  Will also find tail wildcard.
            Node<T> candidate = searcher.eq(matcher);
            if (candidate != null) {
                return candidate;
            }

            // find nodes lt matcher.  Navigate down the trie if there is a partial match.
            candidate = searcher.lt(matcher);
            if (candidate != null && matcher.getFragment().startsWith(candidate.getFragment())) {
                candidate = candidate.findNodeFor(matcher.advance(candidate.fragment.length()));
                if (Matcher.validMatch(candidate)) {
                    return candidate;
                }
            }

            // check for wildcard match after all other tests fail.
            candidate = WildcardRegistry.processWildcards(this, matcher);
            if (Matcher.validMatch(candidate)) {
                return candidate;
            }
        }
        // nothing below this node so return this node.
        return this;
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

    /**
     * A helper class to perform searches on the children of the Node.
     */
    public class Search {
        /**
         * Finds the child node who's fragment matches the fragmentHolder.
         * @param fragmentHolder the fragment to search for.
         * @return matching child node or {@code null} if none match.
         */
        public Node<T> eq(FragmentHolder<String> fragmentHolder) {
            Node<T> test = new Node<>(null, fragmentHolder.getFragment());
            SortedSet<Node<T>> set = children.tailSet(test);
            if (!set.isEmpty()) {
                return fragmentHolder.compareTo(set.first()) == 0 ? set.first() : null;
            }
            return null;
        }

        /**
         * Finds the child node that is less than but closest to the fragmentHolder.
         * @param fragmentHolder the fragment to search for.
         * @return the nearest child less than the fragment or {@code null} if not found.
         */
        public Node<T> lt(FragmentHolder<String> fragmentHolder) {
            Node<T> test = new Node<>(null, fragmentHolder.getFragment());
            SortedSet<Node<T>> set = children.headSet(test);
            return set.isEmpty() ? null : set.last();
        }
    }
}
