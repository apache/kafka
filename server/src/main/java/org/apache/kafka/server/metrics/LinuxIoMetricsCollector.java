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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Retrieves Linux /proc/self/io metrics.
 */
public class LinuxIoMetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(LinuxIoMetricsCollector.class);
    private static final String READ_BYTES_PREFIX = "read_bytes: ";
    private static final String WRITE_BYTES_PREFIX = "write_bytes: ";
    private static final String RCHAR_PREFIX = "rchar: ";
    private static final String WCHAR_PREFIX = "wchar: ";
    private static final String SYSCR_PREFIX = "syscr: ";
    private static final String SYSCW_PREFIX = "syscw: ";
    private static final String CANCELLED_WRITE_BYTES_PREFIX = "cancelled_write_bytes: ";

    private final Time time;
    private final Path path;

    private long lastUpdateMs = -1L;
    private long cachedReadBytes = 0L;
    private long cachedWriteBytes = 0L;
    private long cachedRchar = 0L;
    private long cachedWchar = 0L;
    private long cachedSyscr = 0L;
    private long cachedSyscw = 0L;
    private long cachedCancelledWriteBytes = 0L;

    public LinuxIoMetricsCollector(String procRoot, Time time) {
        this.time = time;
        path = Paths.get(procRoot, "self", "io");
    }

    public long readBytes() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedReadBytes;
        }
    }

    public long writeBytes() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedWriteBytes;
        }
    }

    /**
     * Returns the total number of characters read (includes cached reads).
     * This value represents all read operations, including those satisfied by the page cache.
     */
    public long rchar() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedRchar;
        }
    }

    /**
     * Returns the total number of characters written (includes cached writes).
     * This value represents all write operations, including those that may not have reached disk.
     */
    public long wchar() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedWchar;
        }
    }

    /**
     * Returns the number of read system calls.
     * This metric helps identify I/O patterns and syscall overhead.
     */
    public long syscr() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedSyscr;
        }
    }

    /**
     * Returns the number of write system calls.
     * This metric helps identify I/O patterns and syscall overhead.
     */
    public long syscw() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedSyscw;
        }
    }

    /**
     * Returns the number of bytes that were cancelled before being written.
     * This can occur when a write is truncated or cancelled.
     */
    public long cancelledWriteBytes() {
        synchronized (this) {
            long curMs = time.milliseconds();
            if (curMs != lastUpdateMs) {
                updateValues(curMs);
            }
            return cachedCancelledWriteBytes;
        }
    }

    /**
     * Read /proc/self/io.
     * Generally, each line in this file contains a prefix followed by a colon and a number.
     * For example, it might contain this:
     * rchar: 4052
     * wchar: 0
     * syscr: 13
     * syscw: 0
     * read_bytes: 0
     * write_bytes: 0
     * cancelled_write_bytes: 0
     */
    private boolean updateValues(long now) {
        synchronized (this) {
            try {
                cachedReadBytes = -1L;
                cachedWriteBytes = -1L;
                cachedRchar = -1L;
                cachedWchar = -1L;
                cachedSyscr = -1L;
                cachedSyscw = -1L;
                cachedCancelledWriteBytes = -1L;
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
                for (String line : lines) {
                    if (line.startsWith(READ_BYTES_PREFIX)) {
                        cachedReadBytes = Long.parseLong(line.substring(READ_BYTES_PREFIX.length()));
                    } else if (line.startsWith(WRITE_BYTES_PREFIX)) {
                        cachedWriteBytes = Long.parseLong(line.substring(WRITE_BYTES_PREFIX.length()));
                    } else if (line.startsWith(RCHAR_PREFIX)) {
                        cachedRchar = Long.parseLong(line.substring(RCHAR_PREFIX.length()));
                    } else if (line.startsWith(WCHAR_PREFIX)) {
                        cachedWchar = Long.parseLong(line.substring(WCHAR_PREFIX.length()));
                    } else if (line.startsWith(SYSCR_PREFIX)) {
                        cachedSyscr = Long.parseLong(line.substring(SYSCR_PREFIX.length()));
                    } else if (line.startsWith(SYSCW_PREFIX)) {
                        cachedSyscw = Long.parseLong(line.substring(SYSCW_PREFIX.length()));
                    } else if (line.startsWith(CANCELLED_WRITE_BYTES_PREFIX)) {
                        cachedCancelledWriteBytes = Long.parseLong(line.substring(CANCELLED_WRITE_BYTES_PREFIX.length()));
                    }
                }
                lastUpdateMs = now;
                return true;
            } catch (Throwable t) {
                LOG.warn("Unable to update IO metrics", t);
                return false;
            }
        }
    }

    public boolean usable() {
        if (path.toFile().exists()) {
            return updateValues(time.milliseconds());
        } else {
            LOG.debug("Disabling IO metrics collection because {} does not exist.", path);
            return false;
        }
    }
}
