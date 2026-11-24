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
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean isUpdating = new AtomicBoolean(false);

    private volatile long lastUpdateMs = -1L;
    private volatile long cachedReadBytes = 0L;
    private volatile long cachedWriteBytes = 0L;
    private volatile long cachedRchar = 0L;
    private volatile long cachedWchar = 0L;
    private volatile long cachedSyscr = 0L;
    private volatile long cachedSyscw = 0L;
    private volatile long cachedCancelledWriteBytes = 0L;

    public LinuxIoMetricsCollector(String procRoot, Time time) {
        this.time = time;
        path = Paths.get(procRoot, "self", "io");
    }

    public long readBytes() {
        refreshIfStale();
        return cachedReadBytes;
    }

    public long writeBytes() {
        refreshIfStale();
        return cachedWriteBytes;
    }

    /**
     * Returns the total number of characters read (includes cached reads).
     * This value represents all read operations, including those satisfied by the page cache.
     */
    public long rchar() {
        refreshIfStale();
        return cachedRchar;
    }

    /**
     * Returns the total number of characters written (includes cached writes).
     * This value represents all write operations, including those that may not have reached disk.
     */
    public long wchar() {
        refreshIfStale();
        return cachedWchar;
    }

    /**
     * Returns the number of read system calls.
     * This metric helps identify I/O patterns and syscall overhead.
     */
    public long syscr() {
        refreshIfStale();
        return cachedSyscr;
    }

    /**
     * Returns the number of write system calls.
     * This metric helps identify I/O patterns and syscall overhead.
     */
    public long syscw() {
        refreshIfStale();
        return cachedSyscw;
    }

    /**
     * Returns the number of bytes that were cancelled before being written.
     * This can occur when a write is truncated or cancelled.
     */
    public long cancelledWriteBytes() {
        refreshIfStale();
        return cachedCancelledWriteBytes;
    }

    /**
     * Registers all 7 Linux I/O metrics with the given metrics group. Should be called only
     * after {@link #usable()} returns true.
     */
    public void registerMetrics(KafkaMetricsGroup metricsGroup) {
        metricsGroup.newGauge("linux-disk-read-bytes", this::readBytes);
        metricsGroup.newGauge("linux-disk-write-bytes", this::writeBytes);
        metricsGroup.newGauge("linux-disk-rchar", this::rchar);
        metricsGroup.newGauge("linux-disk-wchar", this::wchar);
        metricsGroup.newGauge("linux-disk-syscr", this::syscr);
        metricsGroup.newGauge("linux-disk-syscw", this::syscw);
        metricsGroup.newGauge("linux-disk-cancelled-write-bytes", this::cancelledWriteBytes);
    }

    /**
     * Refreshes all cached values from /proc/self/io if more than one millisecond has elapsed
     * since the last refresh. Uses an {@link AtomicBoolean} CAS to coordinate so that only
     * one thread performs the update at a time; other concurrent callers fall through and
     * read whatever is currently cached. This is safe because {@link #updateValues} never
     * publishes intermediate {@code -1L} state to the cached fields.
     */
    private void refreshIfStale() {
        long currentMs = time.milliseconds();
        if (currentMs != lastUpdateMs) {
            if (isUpdating.compareAndSet(false, true)) {
                try {
                    currentMs = time.milliseconds();
                    if (currentMs != lastUpdateMs) {
                        updateValues(currentMs);
                    }
                } finally {
                    isUpdating.set(false);
                }
            }
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
     *
     * <p>Parses into local variables first, then publishes to the volatile cached fields only
     * after parsing succeeds. This prevents lock-free readers from observing the transient
     * -1L state during an update. {@code lastUpdateMs} is written last on the success path so
     * any thread observing the new timestamp via a volatile read also observes all updated
     * cached fields.
     *
     * <p>On parse/IO failure, the cached fields are left untouched (readers retain the
     * last-known-good values) but {@code lastUpdateMs} is still updated to {@code now} so that
     * subsequent getters within the same ms-window do not re-trigger the failed read and
     * re-log the warning. This bounds log output to at most one warning per ms-window
     * (i.e. once per JMX scrape) when the file is persistently unreadable.
     */
    private boolean updateValues(long now) {
        long readBytes = -1L;
        long writeBytes = -1L;
        long rchar = -1L;
        long wchar = -1L;
        long syscr = -1L;
        long syscw = -1L;
        long cancelledWriteBytes = -1L;
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.startsWith(READ_BYTES_PREFIX)) {
                    readBytes = Long.parseLong(line.substring(READ_BYTES_PREFIX.length()));
                } else if (line.startsWith(WRITE_BYTES_PREFIX)) {
                    writeBytes = Long.parseLong(line.substring(WRITE_BYTES_PREFIX.length()));
                } else if (line.startsWith(RCHAR_PREFIX)) {
                    rchar = Long.parseLong(line.substring(RCHAR_PREFIX.length()));
                } else if (line.startsWith(WCHAR_PREFIX)) {
                    wchar = Long.parseLong(line.substring(WCHAR_PREFIX.length()));
                } else if (line.startsWith(SYSCR_PREFIX)) {
                    syscr = Long.parseLong(line.substring(SYSCR_PREFIX.length()));
                } else if (line.startsWith(SYSCW_PREFIX)) {
                    syscw = Long.parseLong(line.substring(SYSCW_PREFIX.length()));
                } else if (line.startsWith(CANCELLED_WRITE_BYTES_PREFIX)) {
                    cancelledWriteBytes = Long.parseLong(line.substring(CANCELLED_WRITE_BYTES_PREFIX.length()));
                }
            }
        } catch (Throwable t) {
            LOG.warn("Unable to update IO metrics", t);
            lastUpdateMs = now;
            return false;
        }
        cachedReadBytes = readBytes;
        cachedWriteBytes = writeBytes;
        cachedRchar = rchar;
        cachedWchar = wchar;
        cachedSyscr = syscr;
        cachedSyscw = syscw;
        cachedCancelledWriteBytes = cancelledWriteBytes;
        lastUpdateMs = now;
        return true;
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
