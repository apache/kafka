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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A LinkedHashSet which is more memory-efficient than the standard implementation.
 *
 * This set preserves the order of insertion.  The order of iteration will always be
 * the order of insertion.
 *
 * This collection requires previous and next indexes to be embedded into each
 * element.  Using array indices rather than pointers saves space on large heaps
 * where pointer compression is not in use.  It also reduces the amount of time
 * the garbage collector has to spend chasing pointers.
 *
 * This class uses linear probing.  Unlike HashMap (but like HashTable), we don't force
 * the size to be a power of 2.  This saves memory.
 *
 * This class does not have internal synchronization.
 */
@SuppressWarnings("unchecked")
public class ImplicitLinkedHashSet<E extends ImplicitLinkedHashSet.Element> extends AbstractSet<E> {
    public interface Element {
        int prev();
        void setPrev(int e);
        int next();
        void setNext(int e);
    }

    private static final int HEAD_INDEX = -1;

    public static final int INVALID_INDEX = -2;

    private static class HeadElement implements Element {
        private int prev = HEAD_INDEX;
        private int next = HEAD_INDEX;

        @Override
        public int prev() {
            return prev;
        }

        @Override
        public void setPrev(int prev) {
            this.prev = prev;
        }

        @Override
        public int next() {
            return next;
        }

        @Override
        public void setNext(int next) {
            this.next = next;
        }
    }

    private static Element indexToElement(Element head, Element[] elements, int index) {
        if (index == HEAD_INDEX) {
            return head;
        }
        return elements[index];
    }

    private static void addToListTail(Element head, Element[] elements, int elementIdx) {
        int oldTailIdx = head.prev();
        Element element = indexToElement(head, elements, elementIdx);
        Element oldTail = indexToElement(head, elements, oldTailIdx);
        head.setPrev(elementIdx);
        oldTail.setNext(elementIdx);
        element.setPrev(oldTailIdx);
        element.setNext(HEAD_INDEX);
    }

    private static void removeFromList(Element head, Element[] elements, int elementIdx) {
        Element element = indexToElement(head, elements, elementIdx);
        elements[elementIdx] = null;
        int prevIdx = element.prev();
        int nextIdx = element.next();
        Element prev = indexToElement(head, elements, prevIdx);
        Element next = indexToElement(head, elements, nextIdx);
        prev.setNext(nextIdx);
        next.setPrev(prevIdx);
        element.setNext(INVALID_INDEX);
        element.setPrev(INVALID_INDEX);
    }

    private class ImplicitLinkedHashSetIterator implements Iterator<E> {
        private Element cur = head;

        private Element next = indexToElement(head, elements, head.next());

        @Override
        public boolean hasNext() {
            return next != head;
        }

        @Override
        public E next() {
            if (next == head) {
                throw new NoSuchElementException();
            }
            cur = next;
            next = indexToElement(head, elements, cur.next());
            return (E) cur;
        }

        @Override
        public void remove() {
            if (cur == head) {
                throw new IllegalStateException();
            }
            ImplicitLinkedHashSet.this.remove(cur);
            cur = head;
        }
    }

    private Element head;

    private Element[] elements;

    private int size;

    @Override
    public Iterator<E> iterator() {
        return new ImplicitLinkedHashSetIterator();
    }

    private static int slot(Element[] curElements, Element e) {
        return (e.hashCode() & 0x7fffffff) % curElements.length;
    }

    /**
     * Find an element matching an example element.
     *
     * Using the element's hash code, we can look up the slot where it belongs.
     * However, it may not have ended up in exactly this slot, due to a collision.
     * Therefore, we must search forward in the array until we hit a null, before
     * concluding that the element is not present.
     *
     * @param example   The element to match.
     * @return          The match index, or INVALID_INDEX if no match was found.
     */
    private int findIndex(E example) {
        int slot = slot(elements, example);
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[slot];
            if (element == null) {
                return INVALID_INDEX;
            }
            if (element.equals(example)) {
                return slot;
            }
            slot = (slot + 1) % elements.length;
        }
        return INVALID_INDEX;
    }

    /**
     * Find the element which equals() the given example element.
     *
     * @param example   The example element.
     * @return          Null if no element was found; the element, otherwise.
     */
    public E find(E example) {
        int index = findIndex(example);
        if (index == INVALID_INDEX) {
            return null;
        }
        return (E) elements[index];
    }

    /**
     * Returns the number of elements in the set.
     */
    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(Object o) {
        E example = null;
        try {
            example = (E) o;
        } catch (ClassCastException e) {
            return false;
        }
        return find(example) != null;
    }

    @Override
    public boolean add(E newElement) {
        if ((size + 1) >= elements.length / 2) {
            // Avoid using even-sized capacities, to get better key distribution.
            changeCapacity((2 * elements.length) + 1);
        }
        int slot = addInternal(newElement, elements);
        if (slot >= 0) {
            addToListTail(head, elements, slot);
            size++;
            return true;
        }
        return false;
    }

    public void mustAdd(E newElement) {
        if (!add(newElement)) {
            throw new RuntimeException("Unable to add " + newElement);
        }
    }

    /**
     * Adds a new element to the appropriate place in the elements array.
     *
     * @param newElement    The new element to add.
     * @param addElements   The elements array.
     * @return              The index at which the element was inserted, or INVALID_INDEX
     *                      if the element could not be inserted because there was already
     *                      an equivalent element.
     */
    private static int addInternal(Element newElement, Element[] addElements) {
        int slot = slot(addElements, newElement);
        for (int seen = 0; seen < addElements.length; seen++) {
            Element element = addElements[slot];
            if (element == null) {
                addElements[slot] = newElement;
                return slot;
            }
            if (element.equals(newElement)) {
                return INVALID_INDEX;
            }
            slot = (slot + 1) % addElements.length;
        }
        throw new RuntimeException("Not enough hash table slots to add a new element.");
    }

    private void changeCapacity(int newCapacity) {
        Element[] newElements = new Element[newCapacity];
        HeadElement newHead = new HeadElement();
        int oldSize = size;
        for (Iterator<E> iter = iterator(); iter.hasNext(); ) {
            Element element = iter.next();
            iter.remove();
            int newSlot = addInternal(element, newElements);
            addToListTail(newHead, newElements, newSlot);
        }
        this.elements = newElements;
        this.head = newHead;
        this.size = oldSize;
    }

    @Override
    public boolean remove(Object o) {
        E example = null;
        try {
            example = (E) o;
        } catch (ClassCastException e) {
            return false;
        }
        int slot = findIndex(example);
        if (slot == INVALID_INDEX) {
            return false;
        }
        size--;
        removeFromList(head, elements, slot);
        slot = (slot + 1) % elements.length;

        // Find the next empty slot
        int endSlot = slot;
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[endSlot];
            if (element == null) {
                break;
            }
            endSlot = (endSlot + 1) % elements.length;
        }

        // We must preserve the denseness invariant.  The denseness invariant says that
        // any element is either in the slot indicated by its hash code, or a slot which
        // is not separated from that slot by any nulls.
        // Reseat all elements in between the deleted element and the next empty slot.
        while (slot != endSlot) {
            reseat(slot);
            slot = (slot + 1) % elements.length;
        }
        return true;
    }

    private void reseat(int prevSlot) {
        Element element = elements[prevSlot];
        int newSlot = slot(elements, element);
        for (int seen = 0; seen < elements.length; seen++) {
            Element e = elements[newSlot];
            if ((e == null) || (e == element)) {
                break;
            }
            newSlot = (newSlot + 1) % elements.length;
        }
        if (newSlot == prevSlot) {
            return;
        }
        Element prev = indexToElement(head, elements, element.prev());
        prev.setNext(newSlot);
        Element next = indexToElement(head, elements, element.next());
        next.setPrev(newSlot);
        elements[prevSlot] = null;
        elements[newSlot] = element;
    }

    @Override
    public void clear() {
        reset(elements.length);
    }

    public ImplicitLinkedHashSet() {
        this(5);
    }

    public ImplicitLinkedHashSet(int initialCapacity) {
        reset(initialCapacity);
    }

    private void reset(int capacity) {
        this.head = new HeadElement();
        // Avoid using even-sized capacities, to get better key distribution.
        this.elements = new Element[(2 * capacity) + 1];
        this.size = 0;
    }

    int numSlots() {
        return elements.length;
    }
}
