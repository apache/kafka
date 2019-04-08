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
package org.apache.kafka.streams.state.internals;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Before;
import org.junit.Test;

public class TimeAwareNamedCacheTest {
    private TimeAwareNamedCache cache;
    private Metrics innerMetrics;
    private StreamsMetricsImpl metrics;
    private final String taskIDString = "0.0";
    private final String underlyingStoreName = "storeName";

    @Before
    public void setup() {
        innerMetrics = new Metrics();
        metrics = new MockStreamsMetrics(innerMetrics);
        cache = new TimeAwareNamedCache(taskIDString + "-" + underlyingStoreName, metrics);
    }

    @Test
    public void testRemovalOfExpiredEntries() {
        final List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        byte[] key = toInsert.get(0).key.getBytes();
        byte[] value = toInsert.get(0).value.getBytes();
        final Bytes firstKey = Bytes.wrap(key);
        cache.put(firstKey, 
                  new LRUCacheEntry(value, null, true, 1, 1, 1, ""), 
                  Time.SYSTEM.timer(500));
        for (int i = 1; i < 5; i++) {
            key = toInsert.get(i).key.getBytes();
            value = toInsert.get(i).value.getBytes();
            cache.put(Bytes.wrap(key), new LRUCacheEntry(value, null, true, 1, 1, 1, ""));
        }
        cache.get(firstKey);
        try {
            Thread.sleep(500);
            cache.evict();
        } catch (final InterruptedException e) { 
            //very small chance of this happening
            System.out.println("Sleep interrupted.");
        }
        assertEquals(cache.get(firstKey), null);
    }
}
