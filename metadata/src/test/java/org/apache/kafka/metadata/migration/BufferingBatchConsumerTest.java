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
        consumer.close();
        assertEquals(batches.size(), 0);
    }

    @Test
    public void testOneBatchSameAsMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 4);
        consumer.accept(Arrays.asList(1, 2, 3, 4));
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4));
        consumer.close();
        assertEquals(batches.size(), 1);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4));
    }

    @Test
    public void testOneBatchSmallerThanMinSize() {
        List<List<Integer>> batches = new ArrayList<>();
        BufferingBatchConsumer<Integer> consumer = new BufferingBatchConsumer<>(batches::add, 4);
        consumer.accept(Arrays.asList(1, 2, 3));
        assertEquals(batches.size(), 0);
        consumer.close();
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
        consumer.close();
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
        consumer.close();
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
        consumer.close();
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
        consumer.close();
        assertEquals(batches.size(), 2);
        assertEquals(batches.get(0), Arrays.asList(1, 2, 3, 4, 5, 6));
        assertEquals(batches.get(1), Arrays.asList(7, 8, 9, 10));
    }
}
