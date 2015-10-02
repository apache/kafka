/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.internals.Stamped;

import java.util.Iterator;

public class WindowSupport {

    public static class ValueList<V> {
        Value<V> head = null;
        Value<V> tail = null;
        Value<V> dirty = null;

        public void add(int slotNum, V value, long timestamp) {
            Value<V> v = new Value<>(slotNum, value, timestamp);
            if (tail != null) {
                tail.next = v;
            } else {
                head = v;
            }
            tail = v;
            if (dirty == null) dirty = v;
        }

        public Value<V> first() {
            return head;
        }

        public void removeFirst() {
            if (head != null) {
                if (head == tail) tail = null;
                head = head.next;
            }
        }

        public boolean isEmpty() {
            return head == null;
        }

        public boolean hasDirtyValues() {
            return dirty != null;
        }

        public void clearDirtyValues() {
            dirty = null;
        }

        public Iterator<Value<V>> iterator() {
            return new ValueListIterator<V>(head);
        }

        public Iterator<Value<V>> dirtyValueIterator() {
            return new ValueListIterator<V>(dirty);
        }

    }

    private static class ValueListIterator<V> implements Iterator<Value<V>> {

        Value<V> ptr;

        ValueListIterator(Value<V> start) {
            ptr = start;
        }

        @Override
        public boolean hasNext() {
            return ptr != null;
        }

        @Override
        public Value<V> next() {
            Value<V> value = ptr;
            if (value != null) ptr = value.next;
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    public static class Value<V> extends Stamped<V> {
        public final int slotNum;
        private Value<V> next = null;

        Value(int slotNum, V value, long timestamp) {
            super(value, timestamp);
            this.slotNum = slotNum;
        }
    }


    public static long getLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | bytes[offset + i];
        }
        return value;
    }

    public static int getInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | bytes[offset + i];
        }
        return value;
    }

    public static int putLong(byte[] bytes, int offset, long value) {
        for (int i = 7; i >= 0; i--) {
            bytes[offset + i] = (byte) (value & 0xFF);
            value = value >> 8;
        }
        return 8;
    }

    public static int putInt(byte[] bytes, int offset, int value) {
        for (int i = 3; i >= 0; i--) {
            bytes[offset + i] = (byte) (value & 0xFF);
            value = value >> 8;
        }
        return 4;
    }

    public static int puts(byte[] bytes, int offset, byte[] value) {
        offset += putInt(bytes, offset, value.length);
        System.arraycopy(bytes, offset, value, 0, value.length);
        return 4 + value.length;
    }


    public static <T> T deserialize(byte[] bytes, int offset, int length, String topic, Deserializer<T> deserializer) {
        byte[] buf = new byte[length];
        System.arraycopy(bytes, offset, buf, 0, length);
        return deserializer.deserialize(topic, buf);
    }

}
