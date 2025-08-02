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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for enhanced metrics collection and performance monitoring in Kafka Streams.
 * Provides lightweight performance tracking for operations and queries.
 */
public final class StreamsMetricsUtils {

    private static final ConcurrentMap<String, OperationMetrics> OPERATION_METRICS = new ConcurrentHashMap<>();
    private static final Time TIME = Time.SYSTEM;

    private StreamsMetricsUtils() {
        // Utility class
    }

    /**
     * Records the execution time of an operation.
     *
     * @param operationName the name of the operation
     * @param executionTimeNs the execution time in nanoseconds
     */
    public static void recordOperationTime(final String operationName, final long executionTimeNs) {
        final OperationMetrics metrics = OPERATION_METRICS.computeIfAbsent(
            operationName, 
            k -> new OperationMetrics()
        );
        metrics.recordExecution(executionTimeNs);
    }

    /**
     * Records a successful operation.
     *
     * @param operationName the name of the operation
     */
    public static void recordSuccess(final String operationName) {
        final OperationMetrics metrics = OPERATION_METRICS.computeIfAbsent(
            operationName, 
            k -> new OperationMetrics()
        );
        metrics.recordSuccess();
    }

    /**
     * Records a failed operation.
     *
     * @param operationName the name of the operation
     */
    public static void recordFailure(final String operationName) {
        final OperationMetrics metrics = OPERATION_METRICS.computeIfAbsent(
            operationName, 
            k -> new OperationMetrics()
        );
        metrics.recordFailure();
    }

    /**
     * Gets the metrics for a specific operation.
     *
     * @param operationName the name of the operation
     * @return the operation metrics, or null if not found
     */
    public static OperationMetrics getOperationMetrics(final String operationName) {
        return OPERATION_METRICS.get(operationName);
    }

    /**
     * Clears all collected metrics.
     */
    public static void clearMetrics() {
        OPERATION_METRICS.clear();
    }

    /**
     * Times an operation and records the execution time.
     *
     * @param operationName the name of the operation
     * @param operation the operation to time
     * @param <T> the return type
     * @return the result of the operation
     */
    public static <T> T timeOperation(final String operationName, final java.util.function.Supplier<T> operation) {
        final long startTime = TIME.nanoseconds();
        try {
            final T result = operation.get();
            recordSuccess(operationName);
            return result;
        } catch (Exception e) {
            recordFailure(operationName);
            throw e;
        } finally {
            final long executionTime = TIME.nanoseconds() - startTime;
            recordOperationTime(operationName, executionTime);
        }
    }

    /**
     * Metrics for a specific operation.
     */
    public static class OperationMetrics {
        private final AtomicLong totalExecutions = new AtomicLong(0);
        private final AtomicLong totalSuccesses = new AtomicLong(0);
        private final AtomicLong totalFailures = new AtomicLong(0);
        private final AtomicLong totalExecutionTimeNs = new AtomicLong(0);
        private final AtomicLong minExecutionTimeNs = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxExecutionTimeNs = new AtomicLong(0);

        void recordExecution(final long executionTimeNs) {
            totalExecutions.incrementAndGet();
            totalExecutionTimeNs.addAndGet(executionTimeNs);
            
            // Update min
            long currentMin = minExecutionTimeNs.get();
            while (executionTimeNs < currentMin && 
                   !minExecutionTimeNs.compareAndSet(currentMin, executionTimeNs)) {
                currentMin = minExecutionTimeNs.get();
            }
            
            // Update max
            long currentMax = maxExecutionTimeNs.get();
            while (executionTimeNs > currentMax && 
                   !maxExecutionTimeNs.compareAndSet(currentMax, executionTimeNs)) {
                currentMax = maxExecutionTimeNs.get();
            }
        }

        void recordSuccess() {
            totalSuccesses.incrementAndGet();
        }

        void recordFailure() {
            totalFailures.incrementAndGet();
        }

        /**
         * Gets the total number of executions.
         *
         * @return total executions
         */
        public long getTotalExecutions() {
            return totalExecutions.get();
        }

        /**
         * Gets the total number of successful executions.
         *
         * @return total successes
         */
        public long getTotalSuccesses() {
            return totalSuccesses.get();
        }

        /**
         * Gets the total number of failed executions.
         *
         * @return total failures
         */
        public long getTotalFailures() {
            return totalFailures.get();
        }

        /**
         * Gets the success rate as a percentage.
         *
         * @return success rate (0.0 to 1.0)
         */
        public double getSuccessRate() {
            final long total = totalExecutions.get();
            return total == 0 ? 0.0 : (double) totalSuccesses.get() / total;
        }

        /**
         * Gets the average execution time in nanoseconds.
         *
         * @return average execution time
         */
        public double getAverageExecutionTimeNs() {
            final long total = totalExecutions.get();
            return total == 0 ? 0.0 : (double) totalExecutionTimeNs.get() / total;
        }

        /**
         * Gets the minimum execution time in nanoseconds.
         *
         * @return minimum execution time
         */
        public long getMinExecutionTimeNs() {
            return minExecutionTimeNs.get() == Long.MAX_VALUE ? 0 : minExecutionTimeNs.get();
        }

        /**
         * Gets the maximum execution time in nanoseconds.
         *
         * @return maximum execution time
         */
        public long getMaxExecutionTimeNs() {
            return maxExecutionTimeNs.get();
        }

        @Override
        public String toString() {
            return String.format(
                "OperationMetrics{executions=%d, successes=%d, failures=%d, successRate=%.2f%%, " +
                "avgTimeNs=%.2f, minTimeNs=%d, maxTimeNs=%d}",
                getTotalExecutions(),
                getTotalSuccesses(),
                getTotalFailures(),
                getSuccessRate() * 100,
                getAverageExecutionTimeNs(),
                getMinExecutionTimeNs(),
                getMaxExecutionTimeNs()
            );
        }
    }
}