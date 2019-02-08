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
 * A memory-efficient hash set which tracks the order of insertion of elements.
 *
 * Like java.util.LinkedHashSet, this collection maintains a linked list of elements.
 * However, rather than using a separate linked list, this collection embeds the next
 * and previous fields into the elements themselves.  This reduces memory consumption,
 * because it means that we only have to store one Java object per element, rather
 * than multiple.
 *
 * The next and previous fields are stored as array indices rather than pointers.
 * This ensures that the fields only take 32 bits, even when pointers are 64 bits.
 * It also makes the garbage collector's job easier, because it reduces the number of
 * pointers that it must chase.
 *
 * This class uses linear probing.  Unlike HashMap (but like HashTable), we don't force
 * the size to be a power of 2.  This saves memory.
 *
 * This set does not allow null elements.  It does not have internal synchronization.
 */
public class ImplicitLinkedHashSet<E extends ImplicitLinkedHashSet.Element> extends AbstractSet<E> {
    public interface Element {
        int prev();
        void setPrev(int prev);
        int next();
        void setNext(int next);
    }

    /**
     * A special index value used to indicate that the next or previous field is
     * the head.
     */
    private static final int HEAD_INDEX = -1;

    /**
     * A special index value used for next and previous indices which have not
     * been initialized.
     */
    public static final int INVALID_INDEX = -2;

    /**
     * The minimum new capacity for a non-empty implicit hash set.
     */
    private static final int MIN_NONEMPTY_CAPACITY = 5;

    /**
     * A static empty array used to avoid object allocations when the capacity is zero.
     */
    private static final Element[] EMPTY_ELEMENTS = new Element[0];

    private static class HeadElement implements Element {
        static final HeadElement EMPTY = new HeadElement();

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
            @SuppressWarnings("unchecked")
            E returnValue = (E) cur;
            return returnValue;
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

    Element[] elements;

    private int size;

    /**
     * Returns an iterator that will yield every element in the set.
     * The elements will be returned in the order that they were inserted in.
     *
     * Do not modify the set while you are iterating over it (except by calling
     * remove on the iterator itself, of course.)
     */
    @Override
    final public Iterator<E> iterator() {
        return new ImplicitLinkedHashSetIterator();
    }

    final int slot(Element[] curElements, Object e) {
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
     * @param key               The element to match.
     * @return                  The match index, or INVALID_INDEX if no match was found.
     */
    final private int findIndexOfEqualElement(Object key) {
        if (key == null) {
            return INVALID_INDEX;
        }
        int slot = slot(elements, key);
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[slot];
            if (element == null) {
                return INVALID_INDEX;
            }
            if (key.equals(element)) {
                return slot;
            }
            slot = (slot + 1) % elements.length;
        }
        return INVALID_INDEX;
    }

    /**
     * An element e in the collection such that e.equals(key) and
     * e.hashCode() == key.hashCode().
     *
     * @param key   The element to match.
     * @return      The matching element, or null if there were none.
     */
    final public E find(E key) {
        int index = findIndexOfEqualElement(key);
        if (index == INVALID_INDEX) {
            return null;
        }
        @SuppressWarnings("unchecked")
        E result = (E) elements[index];
        return result;
    }

    /**
     * Returns the number of elements in the set.
     */
    @Override
    final public int size() {
        return size;
    }

    /**
     * Returns true if there is at least one element e in the collection such
     * that key.equals(e) and key.hashCode() == e.hashCode().
     *
     * @param key       The object to try to match.
     */
    @Override
    final public boolean contains(Object key) {
        return findIndexOfEqualElement(key) != INVALID_INDEX;
    }

    private static int calculateCapacity(int expectedNumElements) {
        // Avoid using even-sized capacities, to get better key distribution.
        int newCapacity = (2 * expectedNumElements) + 1;
        // Don't use a capacity that is too small.
        if (newCapacity < MIN_NONEMPTY_CAPACITY) {
            return MIN_NONEMPTY_CAPACITY;
        }
        return newCapacity;
    }

    /**
     * Add a new element to the collection.
     *
     * @param newElement    The new element.
     *
     * @return              True if the element was added to the collection;
     *                      false if it was not, because there was an existing equal element.
     */
    @Override
    final public boolean add(E newElement) {
        if (newElement == null) {
            return false;
        }
        if ((size + 1) >= elements.length / 2) {
            changeCapacity(calculateCapacity(elements.length));
        }
        int slot = addInternal(newElement, elements);
        if (slot >= 0) {
            addToListTail(head, elements, slot);
            size++;
            return true;
        }
        return false;
    }

    final public void mustAdd(E newElement) {
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
     *                      if the element could not be inserted.
     */
    int addInternal(Element newElement, Element[] addElements) {
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

    /**
     * Remove the first element e such that key.equals(e)
     * and key.hashCode == e.hashCode.
     *
     * @param key       The object to try to match.
     * @return          True if an element was removed; false otherwise.
     */
    @Override
    final public boolean remove(Object key) {
        int slot = findElementToRemove(key);
        if (slot == INVALID_INDEX) {
            return false;
        }
        removeElementAtSlot(slot);
        return true;
    }

    int findElementToRemove(Object key) {
        return findIndexOfEqualElement(key);
    }

    /**
     * Remove an element in a particular slot.
     *
     * @param slot      The slot of the element to remove.
     *
     * @return          True if an element was removed; false otherwise.
     */
    private boolean removeElementAtSlot(int slot) {
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

    /**
     * Create a new ImplicitLinkedHashSet.
     */
    public ImplicitLinkedHashSet() {
        this(0);
    }

    /**
     * Create a new ImplicitLinkedHashSet.
     *
     * @param expectedNumElements   The number of elements we expect to have in this set.
     *                              This is used to optimize by setting the capacity ahead
     *                              of time rather than growing incrementally.
     */
    public ImplicitLinkedHashSet(int expectedNumElements) {
        clear(expectedNumElements);
    }

    /**
     * Create a new ImplicitLinkedHashSet.
     *
     * @param iter                  We will add all the elements accessible through this iterator
     *                              to the set.
     */
    public ImplicitLinkedHashSet(Iterator<E> iter) {
        clear(0);
        while (iter.hasNext()) {
            mustAdd(iter.next());
        }
    }

    /**
     * Removes all of the elements from this set.
     */
    @Override
    final public void clear() {
        clear(elements.length);
    }

    /**
     * Removes all of the elements from this set, and resets the set capacity
     * based on the provided expected number of elements.
     */
    final public void clear(int expectedNumElements) {
        if (expectedNumElements == 0) {
            // Optimize away object allocations for empty sets.
            this.head = HeadElement.EMPTY;
            this.elements = EMPTY_ELEMENTS;
            this.size = 0;
        } else {
            this.head = new HeadElement();
            this.elements = new Element[calculateCapacity(expectedNumElements)];
            this.size = 0;
        }
    }

    // Visible for testing
    final int numSlots() {
        return elements.length;
    }
}
