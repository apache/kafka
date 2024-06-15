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
package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState;

import java.util.function.Supplier;

/**
 * Used by the group and transaction coordinator runtimes, the metrics suite holds partition state gauges and sensors.
 */
public interface CoordinatorRuntimeMetrics extends AutoCloseable {

    /**
     * Called when the partition state changes.
     * @param oldState The old state.
     * @param newState The new state to transition to.
     */
    void recordPartitionStateChange(CoordinatorState oldState, CoordinatorState newState);

    /**
     * Record the partition load metric.
     * @param startTimeMs The partition load start time.
     * @param endTimeMs   The partition load end time.
     */
    void recordPartitionLoadSensor(long startTimeMs, long endTimeMs);

    /**
     * Update the event queue time.
     *
     * @param durationMs The queue time.
     */
    void recordEventQueueTime(long durationMs);

    /**
     * Update the event queue processing time.
     *
     * @param durationMs The event processing time.
     */
    void recordEventQueueProcessingTime(long durationMs);

    /**
     * Record the thread idle time.
     * @param idleTimeMs The idle time in milliseconds.
     */
    void recordThreadIdleTime(long idleTimeMs);

    /**
     * Register the event queue size gauge.
     *
     * @param sizeSupplier The size supplier.
     */
    void registerEventQueueSizeGauge(Supplier<Integer> sizeSupplier);
}
