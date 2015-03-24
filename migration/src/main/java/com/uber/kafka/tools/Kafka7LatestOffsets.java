// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

public interface Kafka7LatestOffsets {

    /**
     * Returns the latest offset for a given Kafka 0.7 topic and partition.
     */
    long get(String topic, int partition);

    void close();
}
