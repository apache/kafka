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

    private final Time time;
    private final Path path;

    private long lastUpdateMs = -1L;
    private long cachedReadBytes = 0L;
    private long cachedWriteBytes = 0L;

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
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
                for (String line : lines) {
                    if (line.startsWith(READ_BYTES_PREFIX)) {
                        cachedReadBytes = Long.parseLong(line.substring(READ_BYTES_PREFIX.length()));
                    } else if (line.startsWith(WRITE_BYTES_PREFIX)) {
                        cachedWriteBytes = Long.parseLong(line.substring(WRITE_BYTES_PREFIX.length()));
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
