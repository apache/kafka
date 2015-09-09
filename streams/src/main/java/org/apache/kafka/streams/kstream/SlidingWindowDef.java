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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.FilteredIterator;
import org.apache.kafka.streams.kstream.internals.WindowSupport;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RestoreFunc;
import org.apache.kafka.streams.processor.internals.Stamped;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class SlidingWindowDef<K, V> implements WindowDef<K, V> {
    private final String name;
    private final long duration;
    private final int maxCount;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public SlidingWindowDef(
            String name,
            long duration,
            int maxCount,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            Deserializer<K> keyDeseriaizer,
            Deserializer<V> valueDeserializer) {
        this.name = name;
        this.duration = duration;
        this.maxCount = maxCount;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyDeserializer = keyDeseriaizer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Window<K, V> instance() {
        return new SlidingWindow();
    }

    public class SlidingWindow extends WindowSupport implements Window<K, V> {
        private final Object lock = new Object();
        private ProcessorContext context;
        private int slotNum; // used as a key for Kafka log compaction
        private LinkedList<K> list = new LinkedList<K>();
        private HashMap<K, ValueList<V>> map = new HashMap<>();

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            RestoreFuncImpl restoreFunc = new RestoreFuncImpl();
            context.register(this, restoreFunc);

            for (ValueList<V> valueList : map.values()) {
                valueList.clearDirtyValues();
            }
            this.slotNum = restoreFunc.slotNum;
        }

        @Override
        public Iterator<V> findAfter(K key, final long timestamp) {
            return find(key, timestamp, timestamp + duration);
        }

        @Override
        public Iterator<V> findBefore(K key, final long timestamp) {
            return find(key, timestamp - duration, timestamp);
        }

        @Override
        public Iterator<V> find(K key, final long timestamp) {
            return find(key, timestamp - duration, timestamp + duration);
        }

        /*
         * finds items in the window between startTime and endTime (both inclusive)
         */
        private Iterator<V> find(K key, final long startTime, final long endTime) {
            final ValueList<V> values = map.get(key);

            if (values == null) {
                return null;
            } else {
                return new FilteredIterator<V, Value<V>>(values.iterator()) {
                    @Override
                    protected V filter(Value<V> item) {
                        if (startTime <= item.timestamp && item.timestamp <= endTime)
                            return item.value;
                        else
                            return null;
                    }
                };
            }
        }

        @Override
        public void put(K key, V value, long timestamp) {
            synchronized (lock) {
                slotNum++;

                list.offerLast(key);

                ValueList<V> values = map.get(key);
                if (values == null) {
                    values = new ValueList<>();
                    map.put(key, values);
                }

                values.add(slotNum, value, timestamp);
            }
            evictExcess();
            evictExpired(timestamp - duration);
        }

        private void evictExcess() {
            while (list.size() > maxCount) {
                K oldestKey = list.pollFirst();

                ValueList<V> values = map.get(oldestKey);
                values.removeFirst();

                if (values.isEmpty()) map.remove(oldestKey);
            }
        }

        private void evictExpired(long cutoffTime) {
            while (true) {
                K oldestKey = list.peekFirst();

                ValueList<V> values = map.get(oldestKey);
                Stamped<V> oldestValue = values.first();

                if (oldestValue.timestamp < cutoffTime) {
                    list.pollFirst();
                    values.removeFirst();

                    if (values.isEmpty()) map.remove(oldestKey);
                } else {
                    break;
                }
            }
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void flush() {
            IntegerSerializer intSerializer = new IntegerSerializer();
            ByteArraySerializer byteArraySerializer = new ByteArraySerializer();

            RecordCollector collector = ((ProcessorContextImpl) context).recordCollector();

            for (Map.Entry<K, ValueList<V>> entry : map.entrySet()) {
                ValueList<V> values = entry.getValue();
                if (values.hasDirtyValues()) {
                    K key = entry.getKey();

                    byte[] keyBytes = keySerializer.serialize(name, key);

                    Iterator<Value<V>> iterator = values.dirtyValueIterator();
                    while (iterator.hasNext()) {
                        Value<V> dirtyValue = iterator.next();
                        byte[] slot = intSerializer.serialize("", dirtyValue.slotNum);
                        byte[] valBytes = valueSerializer.serialize(name, dirtyValue.value);

                        byte[] combined = new byte[8 + 4 + keyBytes.length + 4 + valBytes.length];

                        int offset = 0;
                        offset += putLong(combined, offset, dirtyValue.timestamp);
                        offset += puts(combined, offset, keyBytes);
                        offset += puts(combined, offset, valBytes);

                        if (offset != combined.length)
                            throw new IllegalStateException("serialized length does not match");

                        collector.send(new ProducerRecord<>(name, context.id(), slot, combined), byteArraySerializer, byteArraySerializer);
                    }
                    values.clearDirtyValues();
                }
            }
        }

        @Override
        public void close() {
            // TODO
        }

        @Override
        public boolean persistent() {
            // TODO: should not be persistent, right?
            return false;
        }

        private class RestoreFuncImpl implements RestoreFunc {

            final IntegerDeserializer intDeserializer;
            int slotNum = 0;

            RestoreFuncImpl() {
                intDeserializer = new IntegerDeserializer();
            }

            @Override
            public void apply(byte[] slot, byte[] bytes) {

                slotNum = intDeserializer.deserialize("", slot);

                int offset = 0;
                // timestamp
                long timestamp = getLong(bytes, offset);
                offset += 8;
                // key
                int length = getInt(bytes, offset);
                offset += 4;
                K key = deserialize(bytes, offset, length, name, keyDeserializer);
                offset += length;
                // value
                length = getInt(bytes, offset);
                offset += 4;
                V value = deserialize(bytes, offset, length, name, valueDeserializer);

                put(key, value, timestamp);
            }
        }
    }

}
