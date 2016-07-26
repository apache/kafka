/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.perf;



import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.test.MockMemoryLRUCache;


import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CachingPerformanceBenchmarks {

    private static final String STRING_MESSAGE = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a porttitor felis. In vel dolor.";
    private static final int ONE_MILLION = 1000000;
    private static final int HALF_MILLION = ONE_MILLION / 2;
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();
    private static final Serde<byte[]> BYTES_SERDE = Serdes.ByteArray();
    private static final int STRING_MESSAGE_BYTES = 232;
    private static final int INT_WRAPPER_BYTES = 16;
    private static final byte[][] BYTE_VALS = new byte[HALF_MILLION][];
    private static final long[] LONG_VALS = new long[HALF_MILLION];
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();
    private static final String CONTROL = "control";
    private static final String OBJ_MEMORY = "obj-memory";
    private static final String OBJ_MEMORY_DEEP = "obj-memory-deep";
    private static final String BYTES_SERIALIZE = "bytes-ser";

    /**
     *  The MemoryMeter will use instrumentation if present otherwise it will guess the size
     *  using sun.misc.Unsafe
     *
     *  Instrumentation can be used via this vm arg -javaagent:<PATH>/jamm-0.3.1.jar
     */
    private static org.github.jamm.MemoryMeter memoryMeter = new org.github.jamm.MemoryMeter().withGuessing(org.github.jamm.MemoryMeter.Guess.FALLBACK_UNSAFE);

    public static void main(String[] args) throws Exception {
        int numberIterations = 25;
        Map<String, Long> cacheTimingResults;

        System.out.println("Tests for 500,000 inserts 500K count/500K * memory  max cache size \n");

        cacheTimingResults = putGetCacheTiming(numberIterations, HALF_MILLION, HALF_MILLION);
        System.out.println("Control       500K cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(CONTROL) / numberIterations);
        System.out.println("Object        500K cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(OBJ_MEMORY) / numberIterations);
        System.out.println("Object(Deep)  500K cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(OBJ_MEMORY_DEEP) / numberIterations);
        System.out.println("Bytes         500K cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(BYTES_SERIALIZE) / numberIterations);

        System.out.println("\nTests for 1,000,000 inserts 500K count/500K * memory  max cache size \n");

        cacheTimingResults = putGetCacheTiming(numberIterations, HALF_MILLION, ONE_MILLION);
        System.out.println("Control       1M cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(CONTROL) / numberIterations);
        System.out.println("Object        1M cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(OBJ_MEMORY) / numberIterations);
        System.out.println("Object(Deep)  1M cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(OBJ_MEMORY_DEEP) / numberIterations);
        System.out.println("Bytes         1M cache put/get results " + numberIterations + " iterations ave time (millis) " + (double) cacheTimingResults.get(BYTES_SERIALIZE) / numberIterations);

        System.out.println();
        rawSerializationMemoryTiming();
    }


    private static Map<String, Long> putGetCacheTiming(int numberIteration, int cacheMaxSize, int numberInserts) {

        MockMemoryLRUCache.EldestEntryRemovalListener<Integer, String> intStringThrowingRemovalListener = getThrowingRemovalListener();
        MockMemoryLRUCache.EldestEntryRemovalListener<byte[], byte[]> bytesThrowingRemovalListener = getThrowingRemovalListener();

        Map<String, Long> timingResults = new HashMap<>();
        timingResults.put(CONTROL, 0L);
        timingResults.put(OBJ_MEMORY, 0L);
        timingResults.put(OBJ_MEMORY_DEEP, 0L);
        timingResults.put(BYTES_SERIALIZE, 0L);

        for (int i = 0; i < numberIteration; i++) {

            controlPutGetTiming(cacheMaxSize, numberInserts, intStringThrowingRemovalListener, timingResults);

            memoryPutGetTiming(cacheMaxSize, numberInserts, intStringThrowingRemovalListener, timingResults, false);

            memoryPutGetTiming(cacheMaxSize, numberInserts, intStringThrowingRemovalListener, timingResults, true);

            serializedPutGetTiming(cacheMaxSize, numberInserts, bytesThrowingRemovalListener, timingResults);

        }

        return timingResults;

    }

    private static void serializedPutGetTiming(int cacheMaxSize, int numberInserts, MockMemoryLRUCache.EldestEntryRemovalListener<byte[], byte[]> bytesThrowingRemovalListener, Map<String, Long> timingResults) {
        long elapsed;
        long current;
        MockMemoryLRUCache<byte[], byte[]> bytesCache = getBytesCache(cacheMaxSize);

        if (cacheMaxSize == numberInserts) {
            bytesCache.whenEldestRemoved(bytesThrowingRemovalListener);
        }

        elapsed = insertBytes(bytesCache, numberInserts, new BlackHole<String>());
        current = timingResults.get(BYTES_SERIALIZE);
        timingResults.put(BYTES_SERIALIZE, current + elapsed);
    }

    private static void memoryPutGetTiming(int cacheMaxSize, int numberInserts, MockMemoryLRUCache.EldestEntryRemovalListener<Integer, String> intStringThrowingRemovalListener, Map<String, Long> timingResults, boolean measureDeep) {
        long elapsed;
        long current;
        MockMemoryLRUCache<Integer, String> memoryTrackedCache = getIntKeyStringValueCache(cacheMaxSize * (INT_WRAPPER_BYTES + STRING_MESSAGE_BYTES + 16));
        memoryTrackedCache.setMaxCacheByMemory(true);
        memoryTrackedCache.setIsMeasureDeep(measureDeep);
        String key = measureDeep ? OBJ_MEMORY_DEEP : OBJ_MEMORY;

        if (cacheMaxSize == numberInserts) {
            memoryTrackedCache.whenEldestRemoved(intStringThrowingRemovalListener);
        }

        elapsed = insertIntKeyStringValue(memoryTrackedCache, numberInserts, new BlackHole<String>());
        current = timingResults.get(key);
        timingResults.put(key, current + elapsed);
    }

    private static void controlPutGetTiming(int cacheMaxSize, int numberInserts, MockMemoryLRUCache.EldestEntryRemovalListener<Integer, String> intStringThrowingRemovalListener, Map<String, Long> timingResults) {
        long elapsed;
        long current;
        MockMemoryLRUCache<Integer, String> sizeTrackedCache = getIntKeyStringValueCache(cacheMaxSize);

        if (cacheMaxSize == numberInserts) {
            sizeTrackedCache.whenEldestRemoved(intStringThrowingRemovalListener);
        }

        elapsed = insertIntKeyStringValue(sizeTrackedCache, numberInserts, new BlackHole<String>());
        current = timingResults.get(CONTROL);
        timingResults.put(CONTROL, current + elapsed);
    }

    private static void rawSerializationMemoryTiming() throws Exception {
        System.out.println("Raw timing of tracking memory (deep) for 500K Strings");
        long memoryTrackTime = simpleMemoryDeepTrackingTiming();
        System.out.println("Took [" + memoryTrackTime + "] millis to track memory\n");

        System.out.println("Raw timing of tracking memory for 500K Strings");
        memoryTrackTime = simpleMemoryTrackingTiming();
        System.out.println("Took [" + memoryTrackTime + "] millis to track memory\n");

        System.out.println("Raw timing of tracking memory (deep) for 500K ComplexObjects");
        memoryTrackTime = complexMemoryDeepTrackingTiming();
        System.out.println("Took [" + memoryTrackTime + "] millis to track memory\n");

        System.out.println("Raw timing of tracking memory for 500K ComplexObjects");
        memoryTrackTime = complexMemoryTrackingTiming();
        System.out.println("Took [" + memoryTrackTime + "] millis to track memory\n");

        System.out.println("Raw timing of serialization for 500K Strings");
        long serializationTime = simpleSerializationTiming();
        System.out.println("Took [" + serializationTime + "] millis to serialize\n");

        System.out.println("Raw timing of serialization for 500K ComplexObjects");
        serializationTime = complexSerializationTiming();
        System.out.println("Took [" + serializationTime + "] millis to serialize\n");
    }

    private static long insertIntKeyStringValue(MockMemoryLRUCache<Integer, String> cache, int count, BlackHole<String> blackHole) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            cache.put(i, STRING_MESSAGE + i);
            blackHole.takeValue(cache.get(i));
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long insertBytes(MockMemoryLRUCache<byte[], byte[]> cache, int count, BlackHole<String> blackHole) {

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            byte[] keyBytes = INTEGER_SERDE.serializer().serialize("topic", i);
            byte[] valBytes = STRING_SERDE.serializer().serialize("topic", STRING_MESSAGE + i);
            cache.put(keyBytes, valBytes);
            byte[] retrieved = cache.get(keyBytes);
            blackHole.takeValue(STRING_SERDE.deserializer().deserialize("topic", retrieved));
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long complexMemoryDeepTrackingTiming() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < HALF_MILLION; i++) {
            LONG_VALS[i] = memoryMeter.measureDeep(new ComplexObject());
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long simpleMemoryDeepTrackingTiming() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < HALF_MILLION; i++) {
            LONG_VALS[i] = memoryMeter.measureDeep(STRING_MESSAGE + i);
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long complexMemoryTrackingTiming() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < HALF_MILLION; i++) {
            LONG_VALS[i] = memoryMeter.measure(new ComplexObject());
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long simpleMemoryTrackingTiming() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < HALF_MILLION; i++) {
            LONG_VALS[i] = memoryMeter.measure(STRING_MESSAGE + i);
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long complexSerializationTiming() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < HALF_MILLION; i++) {
            byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(new ComplexObject());
            BYTE_VALS[i] = bytes;
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static long simpleSerializationTiming() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < HALF_MILLION; i++) {
            byte[] bytes = STRING_SERDE.serializer().serialize("topic", STRING_MESSAGE + i);
            BYTE_VALS[i] = bytes;
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static MockMemoryLRUCache<byte[], byte[]> getBytesCache(int size) {
        return getCache("bytesCache", size, BYTES_SERDE, BYTES_SERDE);
    }

    private static MockMemoryLRUCache<Integer, String> getIntKeyStringValueCache(int size) {
        return getCache("objectCache", size, INTEGER_SERDE, STRING_SERDE);
    }

    private static <K, V> MockMemoryLRUCache<K, V> getCache(String name, int size, Serde<K> keySerde, Serde<V> valueSerde) {
        return new MockMemoryLRUCache<>(name, size, keySerde, valueSerde);
    }

    private static <K, V> MockMemoryLRUCache.EldestEntryRemovalListener<K, V> getThrowingRemovalListener() {
        return new MockMemoryLRUCache.EldestEntryRemovalListener<K, V>() {
            @Override
            public void apply(K key, V value) {
                throw new IllegalStateException("Should be no evictions");
            }
        };
    }

    private static class BlackHole<T> {
        public  void takeValue(T value) {
            value = null;
        }
    }


    private static class ComplexObject {
        String name = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a porttitor felis.";
        Date date;
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
        static int counter = 0;

        public ComplexObject() {
            this.name = this.name + "_" + counter++;
            this.date = new Date();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        public List<Integer> getNumbers() {
            return numbers;
        }

        public void setNumbers(List<Integer> numbers) {
            this.numbers = numbers;
        }

        public static int getCounter() {
            return counter;
        }

        public static void setCounter(int counter) {
            ComplexObject.counter = counter;
        }
    }

}
