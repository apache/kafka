/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.kafka.connect.mirror.test;
 
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
 
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
 
/**
 * Test double for KafkaConsumer that avoids Mockito instrumentation issues on Java 25.
 * Extends KafkaConsumer so it can be passed to detectors that require KafkaConsumer.
 */
public class TestConsumerStub extends KafkaConsumer<byte[], byte[]> {
 
    private final Map<TopicPartition, Long> beginningOffsetsMap = new HashMap<>();
    private final Map<TopicPartition, Long> endOffsetsMap       = new HashMap<>();
    private final Map<TopicPartition, Long> positionMap         = new HashMap<>();
    private final List<SeekCall>            seekCalls           = new ArrayList<>();
 
    public static class SeekCall {
        public final TopicPartition tp;
        public final long           offset;
        public SeekCall(TopicPartition tp, long offset) {
            this.tp     = tp;
            this.offset = offset;
        }
    }
 
    public TestConsumerStub() {
        super(minimalProps(), null, null);
    }
 
    private static Properties minimalProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9999");
        p.put("key.deserializer",
              "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer",
              "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return p;
    }
 
    // ── Stub setup helpers ────────────────────────────────────────────────────
 
    public TestConsumerStub withBeginningOffsets(TopicPartition tp, long offset) {
        beginningOffsetsMap.put(tp, offset);
        return this;
    }
 
    public TestConsumerStub withEndOffsets(TopicPartition tp, long offset) {
        endOffsetsMap.put(tp, offset);
        return this;
    }
 
    public TestConsumerStub withPosition(TopicPartition tp, long offset) {
        positionMap.put(tp, offset);
        return this;
    }
 
    // ── Seek tracking helpers ─────────────────────────────────────────────────
 
    public List<SeekCall> getSeekCalls()                           { return seekCalls; }
    public int            getSeekCallCount()                       { return seekCalls.size(); }
    public boolean        wasSeekCalled(TopicPartition tp, long o) {
        return seekCalls.stream().anyMatch(c -> c.tp.equals(tp) && c.offset == o);
    }
 
    // ── KafkaConsumer overrides ───────────────────────────────────────────────
 
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : partitions) {
            if (beginningOffsetsMap.containsKey(tp)) result.put(tp, beginningOffsetsMap.get(tp));
        }
        return result;
    }
 
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : partitions) {
            if (endOffsetsMap.containsKey(tp)) result.put(tp, endOffsetsMap.get(tp));
        }
        return result;
    }
 
    @Override
    public long position(TopicPartition partition) {
        return positionMap.getOrDefault(partition, 0L);
    }
 
    @Override
    public void seek(TopicPartition partition, long offset) {
        seekCalls.add(new SeekCall(partition, offset));
    }
 
    @Override public void close() {}
}
 