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

package org.apache.kafka.metadata.migration;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BufferingBatchConsumerTest {

    @Test
    public void testEmptyBatches() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 4);
        consumer.accept(Collections.emptyList());
        consumer.accept(Collections.emptyList());
        assertEquals(batches.size(), 0);
        consumer.flush();
        assertEquals(batches.size(), 0);
    }

    @Test
    public void testOneBatchSameAsMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 4);
        consumer.accept(Arrays.asList(1, 2, 3, 4));
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4));
        consumer.flush();
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4));
    }

    @Test
    public void testOneBatchSmallerThanMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 4);
        consumer.accept(Arrays.asList(1, 2, 3));
        assertEquals(batches.size(), 0);
        consumer.flush();
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3));
    }

    @Test
    public void testOneBatchLargerThanMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 4);
        consumer.accept(Arrays.asList(1, 2, 3, 4, 5));
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5));
        consumer.flush();
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5));
    }

    @Test
    public void testMultiBatchSameAsMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 6);
        consumer.accept(Arrays.asList(1, 2));
        consumer.accept(Arrays.asList(3, 4));
        consumer.accept(Arrays.asList(5, 6));
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5, 6));
        consumer.flush();
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5, 6));
    }

    @Test
    public void testMultiBatchSmallerThanMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 6);
        consumer.accept(Arrays.asList(1, 2));
        consumer.accept(Arrays.asList(3, 4));
        consumer.accept(Collections.singletonList(5));
        assertEquals(batches.size(), 0);
        consumer.flush();
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5));
    }

    @Test
    public void testMultiBatchLargerThanMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 6);
        consumer.accept(Arrays.asList(1, 2));
        consumer.accept(Arrays.asList(3, 4));
        consumer.accept(Arrays.asList(5, 6));
        consumer.accept(Arrays.asList(7, 8));
        consumer.accept(Arrays.asList(9, 10));
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5, 6));
        consumer.flush();
        assertEquals(batches.size(), 2);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5, 6));
        assertEquals(batches.get(1), Arrays.asList(7, 8, 9, 10));
    }
}
