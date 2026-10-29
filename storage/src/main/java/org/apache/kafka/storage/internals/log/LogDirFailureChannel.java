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
package org.apache.kafka.storage.internals.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
 * LogDirFailureChannel allows an external thread to block waiting for new offline log dirs.
 *
 * There should be a single instance of LogDirFailureChannel accessible by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can add the log directory name
 * to the LogDirFailureChannel using maybeAddOfflineLogDir(). Each log directory will be added only once. After a log
 * directory is added for the first time, a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handle the log failure properly.
 * An offline log directory will stay offline until the broker is restarted.
 */ 
public class LogDirFailureChannel {
    private static final Logger log = LoggerFactory.getLogger(LogDirFailureChannel.class);
    private final ConcurrentMap<String, String> offlineLogDirs;
    private final BlockingQueue<String> offlineLogDirQueue;
    // Log dirs with a read check running, see tryStartReadCheck
    private final Set<String> logDirsWithReadCheck = ConcurrentHashMap.newKeySet();

    public LogDirFailureChannel(int logDirNum) {
        this.offlineLogDirs = new ConcurrentHashMap<>();
        this.offlineLogDirQueue = new ArrayBlockingQueue<>(logDirNum);
    }

    public boolean hasOfflineLogDir(String logDir) {
        return offlineLogDirs.containsKey(logDir);
    }

    /**
     * If the given logDir is not already offline, add it to the set of offline
     * log dirs, log the error and enqueue logDir to the logDirFailureEvent queue.
     * An error in a log dir that is already offline is not logged, so that the
     * failures that follow the first one in a failing log dir do not flood the log.
     * 
     * @param logDir The offline logDir.
     * @param msg Error message.
     * @param e Exception instance.
     */
    public void maybeAddOfflineLogDir(String logDir, String msg, IOException e) {
        if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) {
            log.error(msg, e);
            offlineLogDirQueue.add(logDir);
        }
    }

    /**
     * Start a check of whether the given log dir can still be read, unless it is already offline or a check is already
     * running. Taking the log dir offline is all or nothing, so one check at a time per log dir is enough, and a failing
     * disk then holds at most one thread per log dir. A started check must be finished with {@link #finishReadCheck}.
     *
     * @param logDir The log dir to check.
     * @return true if the check was started, false if the log dir is offline or a check is already running.
     */
    public boolean tryStartReadCheck(String logDir) {
        return !hasOfflineLogDir(logDir) && logDirsWithReadCheck.add(logDir);
    }

    /**
     * Finish a check started with {@link #tryStartReadCheck}.
     *
     * @param logDir The checked log dir.
     */
    public void finishReadCheck(String logDir) {
        logDirsWithReadCheck.remove(logDir);
    }

    /**
     * Get the next offline log dir from logDirFailureEvent queue.
     * The method will wait if necessary until a new offline log directory becomes available
     * 
     * @return The next offline log dir.
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public String takeNextOfflineLogDir() throws InterruptedException {
        return offlineLogDirQueue.take();
    }
}
