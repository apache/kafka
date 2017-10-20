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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Manages offset flush scheduling and execution for SourceTasks.
 * </p>
 * <p>
 * Unlike sink tasks which directly manage their offset flushes in the main poll() thread since
 * they drive the event loop and control (for all intents and purposes) the timeouts, source
 * tasks are at the whim of the connector and cannot be guaranteed to wake up on the necessary
 * schedule. Instead, this class tracks all the active tasks, their schedule for flushes, and
 * ensures they are invoked in a timely fashion.
 * </p>
 */
class SourceTaskOffsetFlusher {
    private static final Logger log = LoggerFactory.getLogger(SourceTaskOffsetFlusher.class);

    private final WorkerConfig config;
    private final ScheduledExecutorService flushExecutorService;
    private final ConcurrentMap<ConnectorTaskId, ScheduledFuture<?>> flushers;

    // visible for testing
    SourceTaskOffsetFlusher(WorkerConfig config,
                            ScheduledExecutorService flushExecutorService,
                            ConcurrentMap<ConnectorTaskId, ScheduledFuture<?>> flushers) {
        this.config = config;
        this.flushExecutorService = flushExecutorService;
        this.flushers = flushers;
    }

    public SourceTaskOffsetFlusher(WorkerConfig config) {
        this(config, Executors.newSingleThreadScheduledExecutor(),
                new ConcurrentHashMap<ConnectorTaskId, ScheduledFuture<?>>());
    }

    public void close(long timeoutMs) {
        flushExecutorService.shutdown();
        try {
            if (!flushExecutorService.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                log.error("Graceful shutdown of offset flusher thread timed out.");
            }
        } catch (InterruptedException e) {
            // ignore and allow to exit immediately
        }
    }

    public void schedule(final ConnectorTaskId id, final WorkerSourceTask workerTask) {
        long flushIntervalMs = config.getLong(WorkerConfig.OFFSET_FLUSH_INTERVAL_MS_CONFIG);
        ScheduledFuture<?> flushFuture = flushExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                flush(workerTask);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        flushers.put(id, flushFuture);
    }

    public void remove(ConnectorTaskId id) {
        final ScheduledFuture<?> task = flushers.remove(id);
        if (task == null)
            return;

        try {
            task.cancel(false);
            if (!task.isDone())
                task.get();
        } catch (CancellationException e) {
            // ignore
            log.trace("Offset flush thread was cancelled by another thread while removing connector task with id: {}", id);
        } catch (ExecutionException | InterruptedException e) {
            throw new ConnectException("Unexpected interruption in SourceTaskOffsetFlusher while removing task with id: " + id, e);
        }
    }

    private void flush(WorkerSourceTask workerTask) {
        log.debug("{} Flushing offsets", workerTask);
        try {
            if (workerTask.flushOffsets()) {
                return;
            }
            log.error("{} Failed to flush offsets", workerTask);
        } catch (Throwable t) {
            // We're very careful about exceptions here since any uncaught exceptions in the flush
            // thread would cause the fixed interval schedule on the ExecutorService to stop running
            // for that task
            log.error("{} Unhandled exception when flushing: ", workerTask, t);
        }
    }
}
