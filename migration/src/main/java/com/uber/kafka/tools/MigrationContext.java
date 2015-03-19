// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Context object for migration job. Used for coordination
 * of migrator and producer threads.
 *
 * This class is thread-safe.
 */
public class MigrationContext {

    private final AtomicBoolean failed;
    private final Set<String> topicsWithCorruptOffset;

    public MigrationContext() {
        this.failed = new AtomicBoolean(false);
        this.topicsWithCorruptOffset = Sets.newHashSet();
    }

    /**
     * Returns true if migration job failed; false otherwise.
     */
    public boolean failed() {
        return failed.get();
    }

    public
    void setFailed() {
        failed.set(true);
    }

    /**
     * Returns a set of Kafka 0.7 topics with corrupt offsets.
     */
    public Set<String> getTopicsWithCorruptOffset() {
        synchronized (topicsWithCorruptOffset) {
            return ImmutableSet.copyOf(topicsWithCorruptOffset);
        }
    }

    public void addTopicWithCorruptOffset(String topic) {
        synchronized (topicsWithCorruptOffset) {
            topicsWithCorruptOffset.add(topic);
        }
    }
}
