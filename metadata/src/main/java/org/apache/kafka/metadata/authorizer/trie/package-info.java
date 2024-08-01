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
/**
 * The classes in this package implement a Radix Tree with wildcard searching capabilities.
 * <p>
 *     The tree structure is defined by the {@link org.apache.kafka.metadata.authorizer.trie.Node} class with a specific
 *     String based key implementation handles by the {@link org.apache.kafka.metadata.authorizer.trie.StringTrie} class.
 * </p>
 * <h2>Node structure</h2>
 * <p>
 *     The Node has a pointer to its parent, a String fragment, a SortedSet of children, and an optional content pointer.
 *     <ul>
 *         <li>The fragment is the portion of the key that this node covers.  The entire key can be reconstructed by pushing the
 *         fragments from this node and each parent node onto a stack and then traversing concatenating the fragments from a
 *         top to bottom traversal of the stack.</li>
 *         <li>There are four types of nodes:
 *         <ul>
 *             <li>Root node - There is only one and it has no parent node.</li>
 *             <li>Leaf nodes - Has no child nodes and have "contents" set.</li>
 *             <li>Pure inner node - has at least one child and does <em>not<</em> have "contents" set.</li>
 *             <li>Inner node - has at least one child and has "contents" set.</li>
 *         </ul></li>
 *         <li>The set of children may be null if this is a leaf node</li>
 *         <li>The content may be null if there is no data at this node.</li>
 *     </ul>
 * </p>
 * <h2>Insertion Strategy</h2>
 * <p>
 *     In general when inserting a key the StringInserter is used to walk the key match it with the nodes as it descends
 *     the tree.  The inserter recognizes wildcard characters, as defined in the {@link org.apache.kafka.metadata.authorizer.trie.WildcardRegistry},
 *     and will force the wildcard characters to be located on their own Node.
 * </p>
 * <p>
 *     The Node class implements the insertion strategy by using the Inserter to determine if the node fragment matches
 *     the current inserter fragment and creates nodes as necessary.
 * </p>
 * <h2>Matching Strategy</h2>
 * <p>
 *     In general when searching for a key the StringMatcher is used to walk the key match it with the nodes as the search
 *     descends the tree.  The matcher contains an "exit predicate" that, when true, stops the match and  returns the node
 *     and tracks where in the key the current match is being attempted.
 * </p>
 * <p>
 *     The Node class implements the matching strategy.  While descending the tree the Node checks for matching DENY Acls.
 *     If one is found it is returned.  Otherwise the node that is the closest literal match for the string being searched
 *     for will be located.  If that node does not have a matching ACL we follow the parent pointers back up the tree until
 *     a node with a matching ACL is located or the root is reached.  If the root is reached</p>
 * <p>
 *     In order to quickly search the ACLs on associated with the node the ACLs are stored in a
 *     NameTrieAuthorizerData.AclContainer.  This container sorts the enclosed ACLs by permission type (with DENY first)
 *     and then pattern type, operation, principal, and host.  This order enables rapid search for DENY on descent and
 *     for ALLOW on later ascent.
 * </p>
 * <p>
 *     During the ascent of the tree wildcards are checked.  This ensures that any located wildcard is the most closely
 *     matching pattern.  The '?' wildcard matches a single character and is a simple skip of the first character in
 *     the pattern, the '*' wildcard matchers multiple characters and so requires an iterative approach to test longer
 *     and longer patterns until the match is found or no match is declared.
 * </p>
 */
package org.apache.kafka.metadata.authorizer.trie;

