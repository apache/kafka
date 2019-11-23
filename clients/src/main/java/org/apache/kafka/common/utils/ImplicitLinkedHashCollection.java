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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A memory efficient collection which supports O(1) lookup of elements, and also tracks their
 * order of insertion.  Unlike in a Set or a Map, multiple elements can be inserted which are equal
 * to each other.  The type of contained objects must be a subclass of
 * ImplicitLinkedHashCollection#Element.  All elements must implement hashCode and equals.
 *
 * The internal data structure is a hash table whose elements form a linked list.  Rather than
 * using a separate linked list, this collection embeds the "next" and "previous fields into the
 * elements themselves.  This reduces memory consumption, because it means that we only have to
 * store one Java object per element, rather than multiple.
 *
 * The next and previous fields are stored as array indices rather than pointers.  This ensures
 * that the fields only take 32 bits, even when pointers are 64 bits.  It also makes the garbage
 * collector's job easier, because it reduces the number of pointers that it must chase.
 *
 * This class uses linear probing.  Unlike HashMap (but like HashTable), we don't force the size to
 * be a power of 2.  This saves memory.
 *
 * This set does not allow null elements.  It does not have internal synchronization.
 */
public class ImplicitLinkedHashCollection<E extends ImplicitLinkedHashCollection.Element> extends AbstractCollection<E> {
    public interface Element {
        int prev();
        void setPrev(int prev);
        int next();
        void setNext(int next);
        Element duplicate();
    }

    /**
     * Compares two elements.
     *
     * If compare(x, y) == true, then x.hashCode() == y.hashCode() must also be true.
     */
    public interface Comparator<T> {
        boolean compare(T x, T y);
    }

    /**
     * Compares two elements using Object#equals().
     */
    public static class ObjectEqualityComparator<T> implements Comparator<T> {
        public static final ObjectEqualityComparator<Object> INSTANCE = new ObjectEqualityComparator<>();

        @Override
        public boolean compare(T x, T y) {
            return x.equals(y);
        }
    }

    /**
     * Compares two elements using reference equality.
     */
    public static class ReferenceEqualityComparator<T> implements Comparator<T> {
        public static final ReferenceEqualityComparator<Object> INSTANCE = new ReferenceEqualityComparator<>();

        @Override
        public boolean compare(T x, T y) {
            return x == y;
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

        @Override
        public Element duplicate() {
            return new HeadElement();
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
        private int cursor = 0;
        private Element cur = head;
        private int lastReturnedSlot = INVALID_INDEX;

        ImplicitLinkedHashCollectionIterator(int index) {
            for (int i = 0; i < index; ++i) {
                cur = indexToElement(head, elements, cur.next());
                cursor++;
            }
        }

        @Override
        public boolean hasNext() {
            return cursor != size;
        }

        @Override
        public boolean hasPrevious() {
            return cursor != 0;
        }

        @Override
        public E next() {
            if (cursor == size) {
                throw new NoSuchElementException();
            }
            lastReturnedSlot = cur.next();
            cur = indexToElement(head, elements, cur.next());
            ++cursor;
            @SuppressWarnings("unchecked")
            E returnValue = (E) cur;
            return returnValue;
        }

        @Override
        public E previous() {
            if (cursor == 0) {
                throw new NoSuchElementException();
            }
            @SuppressWarnings("unchecked")
            E returnValue = (E) cur;
            cur = indexToElement(head, elements, cur.prev());
            lastReturnedSlot = cur.next();
            --cursor;
            return returnValue;
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public int previousIndex() {
            return cursor - 1;
        }

        @Override
        public void remove() {
            if (lastReturnedSlot == INVALID_INDEX) {
                throw new IllegalStateException();
            }

            if (cur == indexToElement(head, elements, lastReturnedSlot)) {
                cursor--;
                cur = indexToElement(head, elements, cur.prev());
            }
            ImplicitLinkedHashCollection.this.removeElementAtSlot(lastReturnedSlot);

            lastReturnedSlot = INVALID_INDEX;
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

    final static int slot(Element[] curElements, Object e) {
        return (e.hashCode() & 0x7fffffff) % curElements.length;
    }

    /**
     * Find the index of an element matching a given target element.
     *
     * Using the element's hash code, we can look up the slot where it belongs.
     * However, it may not have ended up in exactly this slot, due to a collision.
     * Therefore, we must search forward in the array until we hit a null, before
     * concluding that the element is not present.
     *
     * @param target            The element to match.
     * @param comparator        The comparator to use.
     * @return                  The match index, or INVALID_INDEX if no match was found.
     */
    @SuppressWarnings("unchecked")
    final private int findIndex(Object target, Comparator<? super E> comparator) {
        if (target == null || size == 0) {
            return INVALID_INDEX;
        }
        int slot = slot(elements, target);
        for (int seen = 0; seen < elements.length; seen++) {
            Element element = elements[slot];
            if (element == null) {
                return INVALID_INDEX;
            }
            if (comparator.compare((E) target, (E) element)) {
                return slot;
            }
            slot = (slot + 1) % elements.length;
        }
        return INVALID_INDEX;
    }

    /**
     * Return the first element e in the collection such that target.equals(e).
     *
     * @param target        The element to match.
     *
     * @return              The matching element, or null if there were none.
     */
    public E find(E target) {
        return find(target, ObjectEqualityComparator.INSTANCE);
    }

    /**
     * Return the first element e in the collection such that comparator.compare(target, e) == true.
     *
     * @param target        The element to match.
     * @param comparator    How to compare the two elements.
     *
     * @return              The matching element, or null if there were none.
     */
    public E find(E target, Comparator<? super E> comparator) {
        int index = findIndex(target, comparator);
        if (index == INVALID_INDEX) {
            return null;
        }
        @SuppressWarnings("unchecked")
        E result = (E) elements[index];
        return result;
    }

    /**
     * Returns all of the elements e in the collection such that
     * target.equals(e).
     *
     * @param target        The element to match.
     *
     * @return              The matching element, or null if there were none.
     */
    public List<E> findAll(E target) {
        return findAll(target, ObjectEqualityComparator.INSTANCE);
    }

    /**
     * Returns all of the elements e in the collection such that
     * comparator.compare(e, target) == true.
     *
     * @param target    The element to match.
     *
     * @return          All of the matching elements.
     */
    @SuppressWarnings("unchecked")
    public List<E> findAll(E target, Comparator<? super E> comparator) {
        if (target == null || size == 0) {
            return Collections.<E>emptyList();
        }
        ArrayList<E> results = new ArrayList<>();
        int slot = slot(elements, target);
        for (int seen = 0; seen < elements.length; seen++) {
            E element = (E) elements[slot];
            if (element == null) {
                break;
            }
            if (comparator.compare(target, element)) {
                results.add(element);
            }
            slot = (slot + 1) % elements.length;
        }
        return results;
    }

    /**
     * Returns the number of elements in the set.
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns true if there is at least one element e in the collection such
     * that key.equals(e) and key.hashCode() == e.hashCode().
     *
     * @param target    The object to try to match.
     */
    @Override
    public boolean contains(Object target) {
        return findIndex(target, ObjectEqualityComparator.INSTANCE) != INVALID_INDEX;
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
     * @return              True if the element was added to the collection.
     *                      False if the element could not be added because it was null.
     */
    @Override
    public boolean add(E newElement) {
        return addOrReplace(newElement, ReferenceEqualityComparator.INSTANCE);
    }

    /**
     * Add a new element to the collection.
     *
     * @param newElement    The new element.
     * @param comparator    A comparator which will be used to determine if any object is
     *                      similar enough to the new element to be replaced.
     *
     * @return              True if the element was added to the collection.
     *                      False if the element could not be added because it was null.
     */
    final public boolean addOrReplace(E newElement, Comparator<? super E> comparator) {
        if (newElement == null) {
            return false;
        }
        if ((size + 1) >= elements.length / 2) {
            changeCapacity(calculateCapacity(elements.length));
        }
        if (addInternal(head, newElement, elements, comparator)) {
            size++;
        }
        return true;
    }

    /**
     * Adds a new element to the appropriate place in the elements array.
     *
     * @param head          The list head.
     * @param newElement    The new element to add.
     * @param addElements   The elements array.
     * @param comparator    A comparator which will be used to determine if any object is
     *                      similar enough to the new element to be replaced.
     *
     * @returns             True if the size of the collection has increased.
     */
    @SuppressWarnings("unchecked")
    static <E> boolean addInternal(Element head,
                                   Element newElement,
                                   Element[] addElements,
                                   Comparator<? super E> comparator) {
        int slot = slot(addElements, newElement);
        int bestSlot = INVALID_INDEX;
        for (int seen = 0; seen < addElements.length; seen++) {
            Element element = addElements[slot];
            if (element == newElement) {
                // If we find that this object has already been added to the collection,
                // create a clone of the object and add the clone instead.  This is necessary
                // because there is only one set of previous and next pointers contained
                // in each element.  Therefore, the same Java object cannot possibly appear
                // more than once in the list.
                newElement = newElement.duplicate();
            } else if (element == null) {
                // When we hit a null, we know that we have seen all the possible values
                // that might be possible to replace with the new element we are adding.
                // This is because of the denseness invariant: if an element E should be
                // in slot S but ends up in slot T, instead, there will never be a null
                // between S and T.
                if (bestSlot == INVALID_INDEX) {
                    bestSlot = slot;
                }
                break;
            } else if (comparator.compare((E) newElement, (E) element)) {
                // If the current element can be replaced by the new element we are adding,
                // mark it down as the best slot we've found so far.  We don't do the
                // replacement immediately because we want to see all the possible values
                // to make sure that the element we are adding has not already been added.
                // That requires iterating until we hit a null.
                bestSlot = slot;
            }
            slot = (slot + 1) % addElements.length;
        }
        boolean growing = true;
        if (addElements[bestSlot] != null) {
            removeFromList(head, addElements, bestSlot);
            growing = false;
        }
        addElements[bestSlot] = newElement;
        addToListTail(head, addElements, bestSlot);
        return growing;
    }

    private void changeCapacity(int newCapacity) {
        Element[] newElements = new Element[newCapacity];
        HeadElement newHead = new HeadElement();
        int oldSize = size;
        for (Iterator<E> iter = iterator(); iter.hasNext(); ) {
            Element element = iter.next();
            iter.remove();
            addInternal(newHead, element, newElements, ReferenceEqualityComparator.INSTANCE);
        }
        this.elements = newElements;
        this.head = newHead;
        this.size = oldSize;
    }

    /**
     * Remove an element from the collection, using object equality semantics.
     *
     * @param target        The object to try to remove.
     * @return              True if an element was removed; false otherwise.
     */
    @Override
    final public boolean remove(Object target) {
        return remove(target, ObjectEqualityComparator.INSTANCE);
    }

    /**
     * Remove an element from the collection, using the given comparator to test equality.
     *
     * @param target        The object to try to remove.
     * @param comparator    The comparator to use.
     * @return              True if an element was removed; false otherwise.
     */
    final public boolean remove(Object target, Comparator<? super E> comparator) {
        int slot = findIndex(target, comparator);
        if (slot == INVALID_INDEX) {
            return false;
        }
        removeElementAtSlot(slot);
        return true;
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
            add(iter.next());
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

    /**
     * Compares the specified object with this collection for equality. Two
     * {@code ImplicitLinkedHashCollection} objects are equal if they contain the
     * same elements (as determined by the element's {@code equals} method), and
     * those elements were inserted in the same order. Because
     * {@code ImplicitLinkedHashCollectionListIterator} iterates over the elements
     * in insertion order, it is sufficient to call {@code valuesList.equals}.
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
}
