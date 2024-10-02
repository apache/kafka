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
 * An {@link Inserter} for String patterns.
 */
public class StandardInserter extends AbstractInserter {

    /** The pattern to insert */
    private final String pattern;

    /** The starting position of the fragment within the pattern */
    private int position;

    /**
     * Creates StandardInserter starting at the beginning of the pattern.
     * @param pattern
     */
    public StandardInserter(String pattern) {
        this.pattern = pattern;
        this.position = 0;
    }


    @Override
    public String getFragment() {
        return pattern.substring(position);
    }

    @Override
    public boolean isEmpty() {
        return position >= pattern.length();
    }

    @Override
    public StandardInserter advance(int advance) {
        position += advance;
        return this;
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
     * @param node identifies the node to locate.
     * @return the added or found Node.
     */
    public <T> Node<T> insertIn(Node<T> node) {
        // If the inserter is empty then we have found the Node.
        if (isEmpty()) {
            return node;
        }


        String segment = getFragment();
        if (node.getChildren() != null) {
            for (Node<T> child : node.getChildren()) {
                // skip wildcard children

                // segment extends or is equal to child fragment so add child to child.
                if (segment.startsWith(child.getFragment())) {
                    return advance(child.getFragment().length()).insertIn(child);
                }


                if (child.getFragment().startsWith(segment)) {
                    return insertBefore(node, child);
                }

                // check partial match case
                //  ABCD    insert ACAB  yields  A
                //                             +----+
                //                            BCD  CAB
                //
                int limit = Math.min(child.getFragment().length(), segment.length());
                for (int i = 0; i < limit; i++) {
                    if (child.getFragment().charAt(i) != segment.charAt(i)) {
                        if (i == 0) {
                            break;
                        }
                        return splitNode(node, child, i);
                    }
                }
            }
        }
        // no children; create child node of this node with the segment, and continue insert.
        return advance(segment.length()).insertIn(node.createChild(segment));
    }
}
