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
 * Manages offset commit scheduling and execution for SourceTasks.
 * </p>
 * <p>
 * Unlike sink tasks which directly manage their offset commits in the main poll() thread since
 * they drive the event loop and control (for all intents and purposes) the timeouts, source
 * tasks are at the whim of the connector and cannot be guaranteed to wake up on the necessary
 * schedule. Instead, this class tracks all the active tasks, their schedule for commits, and
 * ensures they are invoked in a timely fashion.
 * </p>
 */
class SourceTaskOffsetCommitter {
    private static final Logger log = LoggerFactory.getLogger(SourceTaskOffsetCommitter.class);

    private final WorkerConfig config;
    private final ScheduledExecutorService commitExecutorService;
    private final ConcurrentMap<ConnectorTaskId, ScheduledFuture<?>> committers;

    // visible for testing
    SourceTaskOffsetCommitter(WorkerConfig config,
                              ScheduledExecutorService commitExecutorService,
                              ConcurrentMap<ConnectorTaskId, ScheduledFuture<?>> committers) {
        this.config = config;
        this.commitExecutorService = commitExecutorService;
        this.committers = committers;
    }

    public SourceTaskOffsetCommitter(WorkerConfig config) {
        this(config, Executors.newSingleThreadScheduledExecutor(),
                new ConcurrentHashMap<ConnectorTaskId, ScheduledFuture<?>>());
    }

    public void close(long timeoutMs) {
        commitExecutorService.shutdown();
        try {
            if (!commitExecutorService.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                log.error("Graceful shutdown of offset commitOffsets thread timed out.");
            }
        } catch (InterruptedException e) {
            // ignore and allow to exit immediately
        }
    }

    public void schedule(final ConnectorTaskId id, final WorkerSourceTask workerTask) {
        long commitIntervalMs = config.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
        ScheduledFuture<?> commitFuture = commitExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                commit(workerTask);
            }
        }, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
        committers.put(id, commitFuture);
    }

    public void remove(ConnectorTaskId id) {
        final ScheduledFuture<?> task = committers.remove(id);
        if (task == null)
            return;

        try {
            task.cancel(false);
            if (!task.isDone())
                task.get();
        } catch (CancellationException e) {
            // ignore
            log.trace("Offset commit thread was cancelled by another thread while removing connector task with id: {}", id);
        } catch (ExecutionException | InterruptedException e) {
            throw new ConnectException("Unexpected interruption in SourceTaskOffsetCommitter while removing task with id: " + id, e);
        }
    }

    private void commit(WorkerSourceTask workerTask) {
        log.debug("{} Committing offsets", workerTask);
        try {
            if (workerTask.commitOffsets()) {
                return;
            }
            log.error("{} Failed to commit offsets", workerTask);
        } catch (Throwable t) {
            // We're very careful about exceptions here since any uncaught exceptions in the commit
            // thread would cause the fixed interval schedule on the ExecutorService to stop running
            // for that task
            log.error("{} Unhandled exception when committing: ", workerTask, t);
        }
    }
}
