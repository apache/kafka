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

import java.util.AbstractCollection;
import java.util.AbstractSequentialList;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;

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
public class ImplicitLinkedHashCollection<E extends ImplicitLinkedHashCollection.Element> extends AbstractCollection<E> {
    /**
     * The interface which elements of this collection must implement.  The prev,
     * setPrev, next, and setNext functions handle manipulating the implicit linked
     * list which these elements reside in inside the collection.
     * elementKeysAreEqual() is the function which this collection uses to compare
     * elements.
     */
    public interface Element {
        int prev();
        void setPrev(int prev);
        int next();
        void setNext(int next);
        default boolean elementKeysAreEqual(Object other) {
            return equals(other);
        }
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

    private class ImplicitLinkedHashCollectionIterator implements ListIterator<E> {
        private int index = 0;
        private Element cur;
        private Element lastReturned;

        ImplicitLinkedHashCollectionIterator(int index) {
            this.cur = indexToElement(head, elements, head.next());
            for (int i = 0; i < index; ++i) {
                next();
            }
            this.lastReturned = null;
        }

        @Override
        public boolean hasNext() {
            return cur != head;
        }

        @Override
        public boolean hasPrevious() {
            return indexToElement(head, elements, cur.prev()) != head;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            @SuppressWarnings("unchecked")
            E returnValue = (E) cur;
            lastReturned = cur;
            cur = indexToElement(head, elements, cur.next());
            ++index;
            return returnValue;
        }

        @Override
        public E previous() {
            Element prev = indexToElement(head, elements, cur.prev());
            if (prev == head) {
                throw new NoSuchElementException();
            }
            cur = prev;
            --index;
            lastReturned = cur;
            @SuppressWarnings("unchecked")
            E returnValue = (E) cur;
            return returnValue;
        }

        @Override
        public int nextIndex() {
            return index;
        }

        @Override
        public int previousIndex() {
            return index - 1;
        }

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            Element nextElement = indexToElement(head, elements, lastReturned.next());
            ImplicitLinkedHashCollection.this.removeElementAtSlot(nextElement.prev());
            if (lastReturned == cur) {
                // If the element we are removing was cur, set cur to cur->next.
                cur = nextElement;
            } else {
                // If the element we are removing comes before cur, decrement the index,
                // since there are now fewer entries before cur.
                --index;
            }
            lastReturned = null;
        }

        @Override
        public void set(E e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(E e) {
            throw new UnsupportedOperationException();
        }
    }

    private class ImplicitLinkedHashCollectionListView extends AbstractSequentialList<E> {

        @Override
        public ListIterator<E> listIterator(int index) {
            if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            }

            return ImplicitLinkedHashCollection.this.listIterator(index);
        }

        @Override
        public int size() {
            return size;
        }
    }

    private class ImplicitLinkedHashCollectionSetView extends AbstractSet<E> {

        @Override
        public Iterator<E> iterator() {
            return ImplicitLinkedHashCollection.this.iterator();
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean add(E newElement) {
            return ImplicitLinkedHashCollection.this.add(newElement);
        }

        @Override
        public boolean remove(Object key) {
            return ImplicitLinkedHashCollection.this.remove(key);
        }

        @Override
        public boolean contains(Object key) {
            return ImplicitLinkedHashCollection.this.contains(key);
        }

        @Override
        public void clear() {
            ImplicitLinkedHashCollection.this.clear();
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
        return listIterator(0);
    }

    private ListIterator<E> listIterator(int index) {
        return new ImplicitLinkedHashCollectionIterator(index);
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
        if (key == null || size == 0) {
            return INVALID_INDEX;
        }
        int slot = slot(elements, key);
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[slot];
            if (element == null) {
                return INVALID_INDEX;
            }
            if (element.elementKeysAreEqual(key)) {
                return slot;
            }
            slot = (slot + 1) % elements.length;
        }
        return INVALID_INDEX;
    }

    /**
     * An element e in the collection such that e.elementKeysAreEqual(key) and
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
     * that key.elementKeysAreEqual(e) and key.hashCode() == e.hashCode().
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
        return Math.max(newCapacity, MIN_NONEMPTY_CAPACITY);
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
        if (newElement.prev() != INVALID_INDEX || newElement.next() != INVALID_INDEX) {
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
            if (element.elementKeysAreEqual(newElement)) {
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
     * Remove the first element e such that key.elementKeysAreEqual(e)
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
     * Create a new ImplicitLinkedHashCollection.
     */
    public ImplicitLinkedHashCollection() {
        this(0);
    }

    /**
     * Create a new ImplicitLinkedHashCollection.
     *
     * @param expectedNumElements   The number of elements we expect to have in this set.
     *                              This is used to optimize by setting the capacity ahead
     *                              of time rather than growing incrementally.
     */
    public ImplicitLinkedHashCollection(int expectedNumElements) {
        clear(expectedNumElements);
    }

    /**
     * Create a new ImplicitLinkedHashCollection.
     *
     * @param iter                  We will add all the elements accessible through this iterator
     *                              to the set.
     */
    public ImplicitLinkedHashCollection(Iterator<E> iter) {
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
     * Moves an element which is already in the collection so that it comes last
     * in iteration order.
     */
    final public void moveToEnd(E element) {
        if (element.prev() == INVALID_INDEX || element.next() == INVALID_INDEX) {
            throw new RuntimeException("Element " + element + " is not in the collection.");
        }
        Element prevElement = indexToElement(head, elements, element.prev());
        Element nextElement = indexToElement(head, elements, element.next());
        int slot = prevElement.next();
        prevElement.setNext(element.next());
        nextElement.setPrev(element.prev());
        addToListTail(head, elements, slot);
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

    /**
     * Compares the specified object with this collection for equality. Two
     * {@code ImplicitLinkedHashCollection} objects are equal if they contain the
     * same elements (as determined by the element's {@code equals} method), and
     * those elements were inserted in the same order. Because
     * {@code ImplicitLinkedHashCollectionListIterator} iterates over the elements
     * in insertion order, it is sufficient to call {@code valuesList.equals}.
     *
     * Note that {@link ImplicitLinkedHashMultiCollection} does not override
     * {@code equals} and uses this method as well. This means that two
     * {@code ImplicitLinkedHashMultiCollection} objects will be considered equal even
     * if they each contain two elements A and B such that A.equals(B) but A != B and
     * A and B have switched insertion positions between the two collections. This
     * is an acceptable definition of equality, because the collections are still
     * equal in terms of the order and value of each element.
     *
     * @param o object to be compared for equality with this collection
     * @return true is the specified object is equal to this collection
     */
    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof ImplicitLinkedHashCollection))
            return false;

        ImplicitLinkedHashCollection<?> ilhs = (ImplicitLinkedHashCollection<?>) o;
        return this.valuesList().equals(ilhs.valuesList());
    }

    /**
     * Returns the hash code value for this collection. Because
     * {@code ImplicitLinkedHashCollection.equals} compares the {@code valuesList}
     * of two {@code ImplicitLinkedHashCollection} objects to determine equality,
     * this method uses the @{code valuesList} to compute the has code value as well.
     *
     * @return the hash code value for this collection
     */
    @Override
    public int hashCode() {
        return this.valuesList().hashCode();
    }

    // Visible for testing
    final int numSlots() {
        return elements.length;
    }

    /**
     * Returns a {@link List} view of the elements contained in the collection,
     * ordered by order of insertion into the collection. The list is backed by the
     * collection, so changes to the collection are reflected in the list and
     * vice-versa. The list supports element removal, which removes the corresponding
     * element from the collection, but does not support the {@code add} or
     * {@code set} operations.
     *
     * The list is implemented as a circular linked list, so all index-based
     * operations, such as {@code List.get}, run in O(n) time.
     *
     * @return a list view of the elements contained in this collection
     */
    public List<E> valuesList() {
        return new ImplicitLinkedHashCollectionListView();
    }

    /**
     * Returns a {@link Set} view of the elements contained in the collection. The
     * set is backed by the collection, so changes to the collection are reflected in
     * the set, and vice versa. The set supports element removal and addition, which
     * removes from or adds to the collection, respectively.
     *
     * @return a set view of the elements contained in this collection
     */
    public Set<E> valuesSet() {
        return new ImplicitLinkedHashCollectionSetView();
    }

    public void sort(Comparator<E> comparator) {
        ArrayList<E> array = new ArrayList<>(size);
        Iterator<E> iterator = iterator();
        while (iterator.hasNext()) {
            E e = iterator.next();
            iterator.remove();
            array.add(e);
        }
        array.sort(comparator);
        for (E e : array) {
            add(e);
        }
    }
}
