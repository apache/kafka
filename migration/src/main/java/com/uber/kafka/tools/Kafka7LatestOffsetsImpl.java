// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Wrapper around Kafka 0.7 SimpleConsumer for fetching the latest
 * offset. The shenanigans with class loader is necessary because
 * of class name collisions between Kafka 0.7 and 0.8.
 */
class Kafka7LatestOffsetsImpl implements Kafka7LatestOffsets {

    private static final long LATEST_OFFSET = -1L;
    private static final String KAFKA_07_STATIC_SIMPLE_CONSUMER_CLASS_NAME =
        "kafka.javaapi.consumer.SimpleConsumer";
    private static final int SO_TIMEOUT_MS = 10 * 1000;
    private static final int BUFFER_SIZE_BYTES = 1000 * 1024;
    private static final String LEAF_KAFKA_07_HOST = "localhost";
    private static final int LEAF_KAKFA_07_PORT = 9093;

    private final Object simpleConsumer_07;
    private final Method simpleConsumerGetOffsetBeforeMethod_07;
    private final Method simpleConsumerCloseMethod_07;

    Kafka7LatestOffsetsImpl(ClassLoader cl) throws Exception {
        Class<?> simpleConsumerClass_07 = cl.loadClass(KAFKA_07_STATIC_SIMPLE_CONSUMER_CLASS_NAME);
        Constructor simpleConsumerConstructor_07 = simpleConsumerClass_07.getConstructor(
            String.class, int.class, int.class, int.class);
        simpleConsumer_07 = simpleConsumerConstructor_07.newInstance(
            LEAF_KAFKA_07_HOST, LEAF_KAKFA_07_PORT, SO_TIMEOUT_MS, BUFFER_SIZE_BYTES);
        simpleConsumerGetOffsetBeforeMethod_07 = simpleConsumerClass_07.getMethod(
            "getOffsetsBefore", String.class, int.class, long.class, int.class);
        simpleConsumerCloseMethod_07 = simpleConsumerClass_07.getMethod("close");
    }

    @Override
    public long get(String topic, int partition) {
        long[] offsets = null;
        try {
            offsets = (long[]) simpleConsumerGetOffsetBeforeMethod_07.invoke(
                simpleConsumer_07, topic, partition, LATEST_OFFSET, 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (offsets.length < 1) {
            throw new RuntimeException("Failed to find latest offset for " +
                topic + "_" + partition);
        }
        return offsets[0];
    }

    @Override
    public void close() {
        try {
            simpleConsumerCloseMethod_07.invoke(simpleConsumer_07);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
