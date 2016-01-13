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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.ShutdownableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker thread for a WorkerSinkTask. These classes are very tightly coupled, but separated to
 * simplify testing.
 */
class WorkerSinkTaskThread extends ShutdownableThread {
    private static final Logger log = LoggerFactory.getLogger(WorkerSinkTask.class);

    private final WorkerSinkTask task;
    private long nextCommit;
    private boolean committing;
    private int commitSeqno;
    private long commitStarted;
    private int commitFailures;

    public WorkerSinkTaskThread(WorkerSinkTask task, String name, Time time,
                                WorkerConfig workerConfig) {
        super(name);
        this.task = task;
        this.nextCommit = time.milliseconds() +
                workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        this.commitFailures = 0;
    }

    @Override
    public void execute() {
        try {
            // Try to join and start. If we're interrupted before this completes, bail.
            if (!task.joinConsumerGroupAndStart())
                return;

            while (getRunning()) {
                iteration();
            }

            // Make sure any uncommitted data has committed
            task.commitOffsets(true, -1);
        } catch (Throwable t) {
            task.abort(t);
        }
    }

    public void iteration() {
        long now = task.time().milliseconds();

        // Maybe commit
        if (!committing && now >= nextCommit) {
            committing = true;
            commitSeqno += 1;
            commitStarted = now;
            task.commitOffsets(false, commitSeqno);
            nextCommit += task.workerConfig().getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
        }

        // Check for timed out commits
        long commitTimeout = commitStarted + task.workerConfig().getLong(
                WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);
        if (committing && now >= commitTimeout) {
            log.warn("Commit of {} offsets timed out", task);
            commitFailures++;
            committing = false;
        }

        // And process messages
        long timeoutMs = Math.max(nextCommit - now, 0);
        task.poll(timeoutMs);
    }

    public void onCommitCompleted(Throwable error, long seqno) {
        if (commitSeqno != seqno) {
            log.debug("Got callback for timed out commit {}: {}, but most recent commit is {}",
                    this,
                    seqno, commitSeqno);
        } else {
            if (error != null) {
                log.error("Commit of {} offsets threw an unexpected exception: ", task, error);
                commitFailures++;
            } else {
                log.debug("Finished {} offset commit successfully in {} ms",
                        task, task.time().milliseconds() - commitStarted);
                commitFailures = 0;
            }
            committing = false;
        }
    }

    public int commitFailures() {
        return commitFailures;
    }
}
