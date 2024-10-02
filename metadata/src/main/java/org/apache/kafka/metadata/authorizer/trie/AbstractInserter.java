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

public abstract class AbstractInserter implements Inserter {

    @Override
    public String toString() {
        return getFragment();
    }

    // child extends segment; insert segment as new child and original child as segments's child.
    //     A                              A
    //   +----+                         +----+
    //  BCD   CB   insert "AC" yields  BCD   C
    //                                       B
    //
    protected <T> Node<T> insertBefore(Node<T> node, Node<T> child) {
        String segment = getFragment();
        Node<T> newNode = node.createChild(segment);
        // adds renamed child to newNode.children
        child.rename(newNode, child.getFragment().substring(segment.length()));
        return advance(segment.length()).insertIn(newNode);
    }

    // check partial match case
    //  ABCD    insert ACAB  yields  A
    //                             +----+
    //                            BCD  CAB
    //
    protected <T> Node<T> splitNode(Node<T> node, Node<T> child, int idx) {
        String segment = getFragment();
        // newNode adds inserted children
        Node<T> newNode = node.createChild(child.getFragment().substring(0, idx));
        // rename adds old child to newNode.children
        child.rename(newNode, child.getFragment().substring(idx));
        // newChild contains the remainder of the fragment and is child of newNode,
        Node<T> newChild = newNode.createChild(segment.substring(idx));
        return advance(segment.length()).insertIn(newChild);
    }
}
