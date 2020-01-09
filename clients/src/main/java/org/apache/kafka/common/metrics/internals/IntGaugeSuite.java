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
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Manages a suite of integer Gauges.
 */
public final class IntGaugeSuite<K> implements AutoCloseable {
    /**
     * The log4j logger.
     */
    private final Logger log;

    /**
     * The name of this suite.
     */
    private final String suiteName;

    /**
     * The metrics object to use.
     */
    private final Metrics metrics;

    /**
     * A user-supplied callback which translates keys into unique metric names.
     */
    private final Function<K, MetricName> metricNameCalculator;

    /**
     * The maximum number of gauges that we will ever create at once.
     */
    private final int maxEntries;

    /**
     * A map from keys to gauges.  Protected by the object monitor.
     */
    private final Map<K, StoredIntGauge> gauges;

    /**
     * The keys of gauges that can be removed, since their value is zero.
     * Protected by the object monitor.
     */
    private final Set<K> removable;

    /**
     * A lockless list of pending metrics additions and removals.
     */
    private final ConcurrentLinkedDeque<PendingMetricsChange> pending;

    /**
     * A lock which serializes modifications to metrics.  This lock is not
     * required to create a new pending operation.
     */
    private final Lock modifyMetricsLock;

    /**
     * True if this suite is closed.  Protected by the object monitor.
     */
    private boolean closed;

    /**
     * A pending metrics addition or removal.
     */
    private static class PendingMetricsChange {
        /**
         * The name of the metric to add or remove.
         */
        private final MetricName metricName;

        /**
         * In an addition, this field is the MetricValueProvider to add.
         * In a removal, this field is null.
         */
        private final MetricValueProvider<?> provider;

        PendingMetricsChange(MetricName metricName, MetricValueProvider<?> provider) {
            this.metricName = metricName;
            this.provider = provider;
        }
    }

    /**
     * The gauge object which we register with the metrics system.
     */
    private static class StoredIntGauge implements Gauge<Integer> {
        private final MetricName metricName;
        private int value;

        StoredIntGauge(MetricName metricName) {
            this.metricName = metricName;
            this.value = 1;
        }

        /**
         * This callback is invoked when the metrics system retrieves the value of this gauge.
         */
        @Override
        public synchronized Integer value(MetricConfig config, long now) {
            return value;
        }

        synchronized int increment() {
            return ++value;
        }

        synchronized int decrement() {
            return --value;
        }

        synchronized int value() {
            return value;
        }
    }

    public IntGaugeSuite(Logger log,
                         String suiteName,
                         Metrics metrics,
                         Function<K, MetricName> metricNameCalculator,
                         int maxEntries) {
        this.log = log;
        this.suiteName = suiteName;
        this.metrics = metrics;
        this.metricNameCalculator = metricNameCalculator;
        this.maxEntries = maxEntries;
        this.gauges = new HashMap<>(1);
        this.removable = new HashSet<>();
        this.pending = new ConcurrentLinkedDeque<>();
        this.modifyMetricsLock = new ReentrantLock();
        this.closed = false;
        log.trace("{}: created new gauge suite with maxEntries = {}.",
            suiteName, maxEntries);
    }

    public void increment(K key) {
        synchronized (this) {
            if (closed) {
                log.warn("{}: Attempted to increment {}, but the GaugeSuite was closed.",
                    suiteName, key.toString());
                return;
            }
            StoredIntGauge gauge = gauges.get(key);
            if (gauge != null) {
                // Fast path: increment the existing counter.
                if (gauge.increment() > 0) {
                    removable.remove(key);
                }
                return;
            }
            if (gauges.size() == maxEntries) {
                if (removable.isEmpty()) {
                    log.debug("{}: Attempted to increment {}, but there are already {} entries.",
                        suiteName, key.toString(), maxEntries);
                    return;
                }
                Iterator<K> iter = removable.iterator();
                K keyToRemove = iter.next();
                iter.remove();
                MetricName metricNameToRemove = gauges.get(keyToRemove).metricName;
                gauges.remove(keyToRemove);
                pending.push(new PendingMetricsChange(metricNameToRemove, null));
                log.trace("{}: Removing the metric {}, which has a value of 0.",
                    suiteName, keyToRemove.toString());
            }
            MetricName metricNameToAdd = metricNameCalculator.apply(key);
            gauge = new StoredIntGauge(metricNameToAdd);
            gauges.put(key, gauge);
            pending.push(new PendingMetricsChange(metricNameToAdd, gauge));
            log.trace("{}: Adding a new metric {}.", suiteName, key.toString());
        }
        // Drop the object monitor and perform any pending metrics additions or removals.
        performPendingMetricsOperations();
    }

    /**
     * Perform pending metrics additions or removals.
     * It is important to perform them in order.  For example, we don't want to try
     * to remove a metric that we haven't finished adding yet.
     */
    private void performPendingMetricsOperations() {
        modifyMetricsLock.lock();
        try {
            log.trace("{}: entering performPendingMetricsOperations", suiteName);
            for (PendingMetricsChange change = pending.pollLast();
                 change != null;
                 change = pending.pollLast()) {
                if (change.provider == null) {
                    if (log.isTraceEnabled()) {
                        log.trace("{}: removing metric {}", suiteName, change.metricName);
                    }
                    metrics.removeMetric(change.metricName);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("{}: adding metric {}", suiteName, change.metricName);
                    }
                    metrics.addMetric(change.metricName, change.provider);
                }
            }
            log.trace("{}: leaving performPendingMetricsOperations", suiteName);
        } finally {
            modifyMetricsLock.unlock();
        }
    }

    public synchronized void decrement(K key) {
        if (closed) {
            log.warn("{}: Attempted to decrement {}, but the gauge suite was closed.",
                suiteName, key.toString());
            return;
        }
        StoredIntGauge gauge = gauges.get(key);
        if (gauge == null) {
            log.debug("{}: Attempted to decrement {}, but no such metric was registered.",
                suiteName, key.toString());
        } else {
            int cur = gauge.decrement();
            log.trace("{}: Removed a reference to {}.  {} reference(s) remaining.",
                suiteName, key.toString(), cur);
            if (cur <= 0) {
                removable.add(key);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            log.trace("{}: gauge suite is already closed.", suiteName);
            return;
        }
        closed = true;
        int prevSize = 0;
        for (Iterator<StoredIntGauge> iter = gauges.values().iterator(); iter.hasNext(); ) {
            pending.push(new PendingMetricsChange(iter.next().metricName, null));
            prevSize++;
            iter.remove();
        }
        performPendingMetricsOperations();
        log.trace("{}: closed {} metric(s).", suiteName, prevSize);
    }

    /**
     * Get the maximum number of metrics this suite can create.
     */
    public int maxEntries() {
        return maxEntries;
    }

    // Visible for testing only.
    Metrics metrics() {
        return metrics;
    }

    /**
     * Return a map from keys to current reference counts.
     * Visible for testing only.
     */
    synchronized Map<K, Integer> values() {
        HashMap<K, Integer> values = new HashMap<>();
        for (Map.Entry<K, StoredIntGauge> entry : gauges.entrySet()) {
            values.put(entry.getKey(), entry.getValue().value());
        }
        return values;
    }
}
