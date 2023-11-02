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

package org.apache.kafka.timeline;

import java.util.ArrayList;
import java.util.List;

/**
 * A hash table which uses separate chaining.
 * <br>
 * In order to optimize memory consumption a bit, the common case where there is
 * one element per slot is handled by simply placing the element in the slot,
 * and the case where there are multiple elements is handled by creating an
 * array and putting that in the slot. Java is storing type info in memory
 * about every object whether we want it or not, so let's get some benefit
 * out of it.
 * <br>
 * Arrays and null values cannot be inserted.
 */
@SuppressWarnings("unchecked")
class BaseHashTable<T> {
    /**
     * The maximum load factor we will allow the hash table to climb to before expanding.
     */
    private final static double MAX_LOAD_FACTOR = 0.75f;

    /**
     * The minimum number of slots we can have in the hash table.
     */
    final static int MIN_CAPACITY = 2;

    /**
     * The maximum number of slots we can have in the hash table.
     */
    final static int MAX_CAPACITY = 1 << 30;

    private Object[] elements;
    private int size = 0;

    BaseHashTable(int expectedSize) {
        this.elements = new Object[expectedSizeToCapacity(expectedSize)];
    }

    /**
     * Calculate the capacity we should provision, given the expected size.
     * <br>
     * Our capacity must always be a power of 2, and never less than 2 or more
     * than MAX_CAPACITY.  We use 64-bit numbers here to avoid overflow
     * concerns.
     */
    static int expectedSizeToCapacity(int expectedSize) {
        long minCapacity = (long) Math.ceil((float) expectedSize / MAX_LOAD_FACTOR);
        return Math.max(MIN_CAPACITY,
                (int) Math.min(MAX_CAPACITY, roundUpToPowerOfTwo(minCapacity)));
    }

    private static long roundUpToPowerOfTwo(long i) {
        if (i <= 0) {
            return 0;
        } else if (i > (1L << 62)) {
            throw new ArithmeticException("There are no 63-bit powers of 2 higher than " +
                    "or equal to " + i);
        } else {
            return 1L << -Long.numberOfLeadingZeros(i - 1);
        }
    }

    final int baseSize() {
        return size;
    }

    final Object[] baseElements() {
        return elements;
    }

    final T baseGet(Object key) {
        int slot = findSlot(key, elements.length);
        Object value = elements[slot];
        if (value == null) {
            return null;
        } else if (value instanceof Object[]) {
            T[] array = (T[]) value;
            for (T object : array) {
                if (object.equals(key)) {
                    return object;
                }
            }
            return null;
        } else if (value.equals(key)) {
            return (T) value;
        } else {
            return null;
        }
    }

    final T baseAddOrReplace(T newObject) {
        if (((size + 1) * MAX_LOAD_FACTOR > elements.length) &&
                (elements.length < MAX_CAPACITY)) {
            int newSize = elements.length * 2;
            rehash(newSize);
        }
        int slot = findSlot(newObject, elements.length);
        Object cur = elements[slot];
        if (cur == null) {
            size++;
            elements[slot] = newObject;
            return null;
        } else if (cur instanceof Object[]) {
            T[] curArray = (T[]) cur;
            for (int i =  0; i < curArray.length; i++) {
                T value = curArray[i];
                if (value.equals(newObject)) {
                    curArray[i] = newObject;
                    return value;
                }
            }
            size++;
            T[] newArray = (T[]) new Object[curArray.length + 1];
            System.arraycopy(curArray, 0, newArray, 0, curArray.length);
            newArray[curArray.length] = newObject;
            elements[slot] = newArray;
            return null;
        } else if (cur.equals(newObject)) {
            elements[slot] = newObject;
            return (T) cur;
        } else {
            size++;
            elements[slot] = new Object[] {cur, newObject};
            return null;
        }
    }

    final T baseRemove(Object key) {
        int slot = findSlot(key, elements.length);
        Object object = elements[slot];
        if (object == null) {
            return null;
        } else if (object instanceof Object[]) {
            Object[] curArray = (Object[]) object;
            for (int i = 0; i < curArray.length; i++) {
                if (curArray[i].equals(key)) {
                    size--;
                    if (curArray.length <= 2) {
                        int j = i == 0 ? 1 : 0;
                        elements[slot] = curArray[j];
                    } else {
                        Object[] newArray = new Object[curArray.length - 1];
                        System.arraycopy(curArray, 0, newArray, 0, i);
                        System.arraycopy(curArray, i + 1, newArray, i, curArray.length - 1 - i);
                        elements[slot] = newArray;
                    }
                    return (T) curArray[i];
                }
            }
            return null;
        } else if (object.equals(key)) {
            size--;
            elements[slot] = null;
            return (T) object;
        } else {
            return null;
        }
    }

    /**
     * Expand the hash table to a new size.  Existing elements will be copied to new slots.
     */
    private void rehash(int newSize) {
        Object[] prevElements = elements;
        elements = new Object[newSize];
        List<Object> ready = new ArrayList<>();
        for (int slot = 0; slot < prevElements.length; slot++) {
            unpackSlot(ready, prevElements, slot);
            for (Object object : ready) {
                int newSlot = findSlot(object, elements.length);
                Object cur = elements[newSlot];
                if (cur == null) {
                    elements[newSlot] = object;
                } else if (cur instanceof Object[]) {
                    Object[] curArray = (Object[]) cur;
                    Object[] newArray = new Object[curArray.length + 1];
                    System.arraycopy(curArray, 0, newArray, 0, curArray.length);
                    newArray[curArray.length] = object;
                    elements[newSlot] = newArray;
                } else {
                    elements[newSlot] = new Object[]{cur, object};
                }
            }
            ready.clear();
        }
    }

    /**
     * Find the slot in the array that an element should go into.
     */
    static int findSlot(Object object, int numElements) {
        // This performs a secondary hash using Knuth's multiplicative Fibonacci
        // hashing.  Then, we choose some of the highest bits.  The number of bits
        // we choose is based on the table size.  If the size is 2, we need 1 bit;
        // if the size is 4, we need 2 bits, etc.
        int objectHashCode = object.hashCode();
        int log2size = 32 - Integer.numberOfLeadingZeros(numElements);
        int shift = 65 - log2size;
        return (int) ((objectHashCode * -7046029254386353131L) >>> shift);
    }

    /**
     * Copy any elements in the given slot into the output list.
     */
    static <T> void unpackSlot(List<T> out, Object[] elements, int slot) {
        Object value = elements[slot];
        if (value != null) {
            if (value instanceof Object[]) {
                Object[] array = (Object[]) value;
                for (Object object : array) {
                    out.add((T) object);
                }
            } else {
                out.add((T) value);
            }
        }
    }

    String baseToDebugString() {
        StringBuilder bld = new StringBuilder();
        bld.append("BaseHashTable{");
        for (int i = 0; i < elements.length; i++) {
            Object slotObject = elements[i];
            bld.append(String.format("%n%d: ", i));
            if (slotObject == null) {
                bld.append("null");
            } else if (slotObject instanceof Object[]) {
                Object[] array = (Object[]) slotObject;
                String prefix = "";
                for (Object object : array) {
                    bld.append(prefix);
                    prefix = ", ";
                    bld.append(object);
                }
            } else {
                bld.append(slotObject);
            }
        }
        bld.append(String.format("%n}"));
        return bld.toString();
    }
}
