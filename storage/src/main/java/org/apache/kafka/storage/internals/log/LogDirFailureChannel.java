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

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public LogDirFailureChannel(int logDirNum) {
        this.offlineLogDirs = new ConcurrentHashMap<>();
        this.offlineLogDirQueue = new ArrayBlockingQueue<>(logDirNum);
    }

    public boolean hasOfflineLogDir(String logDir) {
        return offlineLogDirs.containsKey(logDir);
    }

    /**
     * If the given logDir is not already offline, add it to the
     * set of offline log dirs and enqueue it to the logDirFailureEvent queue.
     * 
     * @param logDir The offline logDir.
     * @param msg Error message.
     * @param e Exception instance.
     */
    public void maybeAddOfflineLogDir(String logDir, String msg, IOException e) {
        log.error(msg, e);
        if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) {
            offlineLogDirQueue.add(logDir);
        }
    }

    /**
     * Get the next offline log dir from logDirFailureEvent queue.
     * The method will wait if necessary until a new offline log directory becomes available
     * 
     * @return The next offline log dir.
     * @throws InterruptedException
     */
    public String takeNextOfflineLogDir() throws InterruptedException {
        return offlineLogDirQueue.take();
    }
}
