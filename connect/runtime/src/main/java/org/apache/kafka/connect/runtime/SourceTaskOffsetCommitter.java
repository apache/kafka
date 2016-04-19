/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
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

    private WorkerConfig config;
    private ScheduledExecutorService commitExecutorService = null;
    private final HashMap<ConnectorTaskId, ScheduledCommitTask> committers = new HashMap<>();

    SourceTaskOffsetCommitter(WorkerConfig config) {
        this.config = config;
        commitExecutorService = Executors.newSingleThreadScheduledExecutor();
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
        synchronized (committers) {
            long commitIntervalMs = config.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
            ScheduledFuture<?> commitFuture = commitExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    commit(id, workerTask);
                }
            }, commitIntervalMs, TimeUnit.MILLISECONDS);
            committers.put(id, new ScheduledCommitTask(commitFuture));
        }
    }

    public void remove(ConnectorTaskId id) {
        final ScheduledCommitTask task;
        synchronized (committers) {
            task = committers.remove(id);
            task.cancelled = true;
            task.commitFuture.cancel(false);
        }
        if (task.finishedLatch != null) {
            try {
                task.finishedLatch.await();
            } catch (InterruptedException e) {
                throw new ConnectException("Unexpected interruption in SourceTaskOffsetCommitter.", e);
            }
        }
    }

    private void commit(ConnectorTaskId id, WorkerSourceTask workerTask) {
        final ScheduledCommitTask task;
        synchronized (committers) {
            task = committers.get(id);
            if (task == null || task.cancelled)
                return;
            task.finishedLatch = new CountDownLatch(1);
        }

        try {
            log.debug("Committing offsets for {}", workerTask);
            boolean success = workerTask.commitOffsets();
            if (!success) {
                log.error("Failed to commit offsets for {}", workerTask);
            }
        } catch (Throwable t) {
            // We're very careful about exceptions here since any uncaught exceptions in the commit
            // thread would cause the fixed interval schedule on the ExecutorService to stop running
            // for that task
            log.error("Unhandled exception when committing {}: ", workerTask, t);
        } finally {
            synchronized (committers) {
                task.finishedLatch.countDown();
                if (!task.cancelled)
                    schedule(id, workerTask);
            }
        }
    }

    private static class ScheduledCommitTask {
        ScheduledFuture<?> commitFuture;
        boolean cancelled;
        CountDownLatch finishedLatch;

        ScheduledCommitTask(ScheduledFuture<?> commitFuture) {
            this.commitFuture = commitFuture;
            this.cancelled = false;
            this.finishedLatch = null;
        }
    }
}
