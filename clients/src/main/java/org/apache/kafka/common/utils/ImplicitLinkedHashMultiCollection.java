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

package org.apache.kafka.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A memory-efficient hash multiset which tracks the order of insertion of elements.
 * See org.apache.kafka.common.utils.ImplicitLinkedHashCollection for implementation details.
 *
 * This class is a multi-set because it allows multiple elements to be inserted that are
 * equal to each other.
 *
 * We use reference equality when adding elements to the set.  A new element A can
 * be added if there is no existing element B such that A == B.  If an element B
 * exists such that A.equals(B), A will still be added.
 *
 * When deleting an element A from the set, we will try to delete the element B such
 * that A == B.  If no such element can be found, we will try to delete an element B
 * such that A.equals(B).
 *
 * contains() and find() are unchanged from the base class-- they will look for element
 * based on object equality, not reference equality.
 *
 * This multiset does not allow null elements.  It does not have internal synchronization.
 */
public class ImplicitLinkedHashMultiCollection<E extends ImplicitLinkedHashCollection.Element>
        extends ImplicitLinkedHashCollection<E> {
    public ImplicitLinkedHashMultiCollection() {
        super(0);
    }

    public ImplicitLinkedHashMultiCollection(int expectedNumElements) {
        super(expectedNumElements);
    }

    public ImplicitLinkedHashMultiCollection(Iterator<E> iter) {
        super(iter);
    }


    /**
     * Adds a new element to the appropriate place in the elements array.
     *
     * @param newElement    The new element to add.
     * @param addElements   The elements array.
     * @return              The index at which the element was inserted, or INVALID_INDEX
     *                      if the element could not be inserted.
     */
    @Override
    int addInternal(Element newElement, Element[] addElements) {
        int slot = slot(addElements, newElement);
        for (int seen = 0; seen < addElements.length; seen++) {
            Element element = addElements[slot];
            if (element == null) {
                addElements[slot] = newElement;
                return slot;
            }
            if (element == newElement) {
                return INVALID_INDEX;
            }
            slot = (slot + 1) % addElements.length;
        }
        throw new RuntimeException("Not enough hash table slots to add a new element.");
    }

    /**
     * Find an element matching an example element.
     *
     * @param key               The element to match.
     *
     * @return                  The match index, or INVALID_INDEX if no match was found.
     */
    @Override
    int findElementToRemove(Object key) {
        if (key == null) {
            return INVALID_INDEX;
        }
        int slot = slot(elements, key);
        int bestSlot = INVALID_INDEX;
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[slot];
            if (element == null) {
                return bestSlot;
            }
            if (key == element) {
                return slot;
            } else if (key.equals(element)) {
                bestSlot = slot;
            }
            slot = (slot + 1) % elements.length;
        }
        return INVALID_INDEX;
    }

    /**
     * Returns all of the elements e in the collection such that
     * key.equals(e) and key.hashCode() == e.hashCode().
     *
     * @param key       The element to match.
     *
     * @return          All of the matching elements.
     */
    final public List<E> findAll(E key) {
        if (key == null) {
            return Collections.<E>emptyList();
        }
        ArrayList<E> results = new ArrayList<>();
        int slot = slot(elements, key);
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[slot];
            if (element == null) {
                break;
            }
            if (key.equals(element)) {
                @SuppressWarnings("unchecked")
                E result = (E) elements[slot];
                results.add(result);
            }
            slot = (slot + 1) % elements.length;
        }
        return results;
    }
}
