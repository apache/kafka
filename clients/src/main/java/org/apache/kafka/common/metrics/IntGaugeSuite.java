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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Manages a suite of integer Gauges.
 */
public class IntGaugeSuite<K> implements AutoCloseable {
    private final Logger log;
    private final String suiteName;
    private final Metrics metrics;
    private final Function<K, MetricName> metricNameCalculator;
    private final int maxEntries;
    private final Map<K, StoredIntGauge> map;
    private boolean closed;

    class StoredIntGauge implements Gauge<Integer> {
        private final MetricName metricName;
        int value;

        StoredIntGauge(MetricName metricName) {
            this.metricName = metricName;
            this.value = 0;
        }

        @Override
        public Integer value(MetricConfig config, long now) {
            synchronized (IntGaugeSuite.this) {
                return value;
            }
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
        this.map = new HashMap<>(1);
        this.closed = false;
        log.trace("{}: created new gauge suite with maxEntries = {}.",
            suiteName, maxEntries);
    }

    public synchronized void increment(K shortName) {
        if (closed) {
            log.warn("{}: Attempted to increment {}, but the GaugeSuite was closed.",
                suiteName, shortName.toString());
            return;
        }
        StoredIntGauge gauge = map.get(shortName);
        if (gauge == null) {
            if (map.size() == maxEntries) {
                log.warn("{}: Attempted to increment {}, but there are already {} entries.",
                    suiteName, shortName.toString(), maxEntries);
                return;
            }
            MetricName metricName = metricNameCalculator.apply(shortName);
            gauge = new StoredIntGauge(metricName);
            metrics.addMetric(metricName, gauge);
            log.trace("{}: Added a new metric {}", suiteName, shortName.toString());
            map.put(shortName, gauge);
        }
        gauge.value++;
    }

    public synchronized void decrement(K shortName) {
        if (closed) {
            log.warn("{}: Attempted to decrement {}, but the gauge suite was closed.",
                suiteName, shortName.toString());
            return;
        }
        StoredIntGauge gauge = map.get(shortName);
        if (gauge == null) {
            log.warn("{}: Attempted to decrement {}, but no such metric was registered.",
                suiteName, shortName.toString());
        } else {
            gauge.value--;
            if (gauge.value == 0) {
                log.trace("{}: Removing {}.", suiteName, shortName.toString());
                metrics.removeMetric(gauge.metricName);
                map.remove(shortName);
            } else {
                log.trace("{}: Removed a reference to {}.  {} reference(s) remaining.",
                    suiteName, shortName.toString(), gauge.value);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            int prevSize = map.values().size();
            for (StoredIntGauge gauge : map.values()) {
                metrics.removeMetric(gauge.metricName);
            }
            log.trace("{}: closed {} metric(s).", suiteName, prevSize);
            map.clear();
        } else {
            log.trace("{}: gauge suite is already closed.", suiteName);
        }
    }

    public int maxEntries() {
        return maxEntries;
    }

    // visible for testing only
    synchronized void visit(Consumer<Map<K, StoredIntGauge>> visitor) {
        visitor.accept(map);
    }
}
