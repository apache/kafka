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

package org.apache.kafka.metadata.authorizer;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;


/**
 * A mutable class used to build new prefix trees using a combination of new and old components.
 */
final class PrefixTreeBuilder implements AclChanges {
    private final PrefixNode oldRoot;
    private final TreeMap<String, ResourceAclsChanges> changes;

    PrefixTreeBuilder(
        PrefixNode oldRoot
    ) {
        this.oldRoot = oldRoot;
        this.changes = new TreeMap<>();
    }

    @Override
    public void newAddition(StandardAcl acl) {
        changes.computeIfAbsent(acl.resourceNameForPrefixNode(),
                __ ->  new ResourceAclsChanges()).newAddition(acl);
    }

    @Override
    public void newRemoval(StandardAcl acl) {
        changes.computeIfAbsent(acl.resourceNameForPrefixNode(),
                __ ->  new ResourceAclsChanges()).newRemoval(acl);
    }

    PrefixNode build() {
        return buildNode(oldRoot);
    }

    PrefixNode buildNode(PrefixNode oldNode) {
        // Apply any changes we need to apply to this node itself.
        ResourceAclsChanges nodeChanges = changes.remove(oldNode.name());
        ResourceAcls newResourceAcls;
        if (nodeChanges == null) {
            newResourceAcls = oldNode.resourceAcls();
        } else {
            newResourceAcls = oldNode.resourceAcls().copyWithChanges(nodeChanges);
        }
        NavigableMap<String, PrefixNode> newChildren;
        Entry<String, ResourceAclsChanges> changeEntry = nextChangeForNode(oldNode.name(), oldNode);
        if (changeEntry.getValue() == null) {
            newChildren = oldNode.children();
        } else {
            newChildren = buildNewChildren(changeEntry, oldNode);
        }
        if (!oldNode.isRoot() && newResourceAcls.isEmpty()) {
            if (newChildren.isEmpty()) {
                // If the node has no resourceAcls and no children, return null, indicating that we
                // want to delete it.
                return null;
            } else if (newChildren.size() == 1) {
                // If the node has no resourceAcls and one child, return the child. We will replace
                // the parent with the child.
                return newChildren.values().iterator().next();
            }
        }
        return new PrefixNode(newChildren, oldNode.name(), newResourceAcls);
    }

    Entry<String, ResourceAclsChanges> nextChangeForNode(
        String prevKey,
        PrefixNode parent
    ) {
        Entry<String, ResourceAclsChanges> changeEntry = changes.higherEntry(prevKey);
        if (changeEntry == null || !changeEntry.getKey().startsWith(parent.name())) {
            return new SimpleImmutableEntry<>(prevKey, null);
        }
        return changeEntry;
    }

    Entry<String, PrefixNode> nextChildForNode(
        String prevKey,
        PrefixNode parent
    ) {
        Entry<String, PrefixNode> nextChild = parent.children().higherEntry(prevKey);
        if (nextChild == null) {
            return new SimpleImmutableEntry<>(prevKey, null);
        }
        return nextChild;
    }

    enum ChangeAndChildComparison {
        UNRELATED_CHANGE_COMES_FIRST,
        CHANGE_INSERTS_PARENT_FOR_CHILD,
        CHANGE_AFFECTS_CHILD_OR_GRANDCHILDREN,
        UNRELATED_CHILD_COMES_FIRST,
        BOTH_ARE_NULL;

        static ChangeAndChildComparison create(
            Entry<String, ResourceAclsChanges> changeEntry,
            Entry<String, PrefixNode> childEntry
        ) {
            if (changeEntry.getValue() == null) {
                if (childEntry.getValue() == null) {
                    return BOTH_ARE_NULL;
                } else {
                    return UNRELATED_CHILD_COMES_FIRST;
                }
            } else if (childEntry.getValue() == null) {
                return UNRELATED_CHANGE_COMES_FIRST;
            }
            int comparison = changeEntry.getKey().compareTo(childEntry.getKey());
            if (comparison == 0) {
                return CHANGE_AFFECTS_CHILD_OR_GRANDCHILDREN;
            } else if (comparison < 0) {
                if (childEntry.getKey().startsWith(changeEntry.getKey())) {
                    return CHANGE_AFFECTS_CHILD_OR_GRANDCHILDREN;
                }
                return UNRELATED_CHANGE_COMES_FIRST;
            } else {
                if (changeEntry.getKey().startsWith(childEntry.getKey())) {
                    return CHANGE_INSERTS_PARENT_FOR_CHILD;
                }
                return UNRELATED_CHILD_COMES_FIRST;
            }
        }
    };

    NavigableMap<String, PrefixNode> buildNewChildren(
        Entry<String, ResourceAclsChanges> changeEntry,
        PrefixNode parent
    ) {
        NavigableMap<String, PrefixNode> newChildren = new TreeMap<>();
        Entry<String, PrefixNode> childEntry = nextChildForNode(parent.name(), parent);

        while (true) {
            ChangeAndChildComparison comparison =
                    ChangeAndChildComparison.create(changeEntry, childEntry);
            System.out.println("Compared changeEntry " + changeEntry + " and childEntry " + childEntry + " to get " + comparison);
            switch (comparison) {
                case UNRELATED_CHANGE_COMES_FIRST: {
                    // Either the child is null, or we're handling a change that comes before the
                    // child. In either case, we need to insert a new node to the parent's collection.
                    PrefixNode newChild = buildNode(new PrefixNode(Collections.emptyNavigableMap(),
                            changeEntry.getKey(), ResourceAcls.EMPTY));
                    newChildren.put(newChild.name(), newChild);
                    changeEntry = nextChangeForNode(changeEntry.getKey(), parent);
                    break;
                }
                case CHANGE_INSERTS_PARENT_FOR_CHILD: {
                    // We create a virtual child whose child is the current child and build a new
                    // node based on modifying that.
                    NavigableMap<String, PrefixNode> virtualGrandchildren = new TreeMap<>();
                    virtualGrandchildren.put(childEntry.getKey(), childEntry.getValue());
                    PrefixNode virtualChild = new PrefixNode(virtualGrandchildren,
                            changeEntry.getKey(), ResourceAcls.EMPTY);
                    PrefixNode newChild = buildNode(virtualChild);
                    newChildren.put(newChild.name(), newChild);
                    // Skip over the current direct child, which is now the child of the new
                    // child we just created.
                    childEntry = nextChildForNode(childEntry.getKey(), parent);
                    changeEntry = nextChangeForNode(changeEntry.getKey(), parent);
                    break;
                }
                case CHANGE_AFFECTS_CHILD_OR_GRANDCHILDREN: {
                    // The change relates to the child. This may mean it changes the child itself,
                    // or changes one of the child's children.
                    PrefixNode newChild = buildNode(childEntry.getValue());
                    if (newChild == null) {
                        // If buildNode returns null, we're deleting this child.
                        // Advance to the next child, if there is one.
                        childEntry = nextChildForNode(childEntry.getKey(), parent);
                    } else {
                        // Insert the new child we just built. Note that this new child may have a
                        // different name than the previous one, if a layer was removed.
                        newChildren.put(newChild.name(), newChild);
                        childEntry = nextChildForNode(newChild.name(), parent);
                    }
                    changeEntry = nextChangeForNode(changeEntry.getKey(), parent);
                    break;
                }
                case UNRELATED_CHILD_COMES_FIRST: {
                    // Preserve the previous child entry unmodified. This is always nice because
                    // we don't have to do any more work on this subtree.
                    newChildren.put(childEntry.getKey(), childEntry.getValue());
                    childEntry = nextChildForNode(childEntry.getKey(), parent);
                    break;
                }
                case BOTH_ARE_NULL:
                    // We're done.
                    return newChildren;
            }
        }
    }
}
