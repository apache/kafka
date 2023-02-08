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

/**
 * Configuration parameters for the log cleaner.
 */
public class CleanerConfig {
    public static final String HASH_ALGORITHM = "MD5";

    public final int numThreads;
    public final long dedupeBufferSize;
    public final double dedupeBufferLoadFactor;
    public final int ioBufferSize;
    public final int maxMessageSize;
    public final double maxIoBytesPerSecond;
    public final long backoffMs;
    public final boolean enableCleaner;

    public CleanerConfig(boolean enableCleaner) {
        this(1, 4 * 1024 * 1024, 0.9, 1024 * 1024,
            32 * 1024 * 1024, Double.MAX_VALUE, 15 * 1000, enableCleaner);
    }

    /**
     * Create an instance of this class.
     *
     * @param numThreads The number of cleaner threads to run
     * @param dedupeBufferSize The total memory used for log deduplication
     * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param maxMessageSize The maximum size of a message that can appear in the log
     * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do
     * @param backoffMs The amount of time to wait before rechecking if no logs are eligible for cleaning
     * @param enableCleaner Allows completely disabling the log cleaner
     */
    public CleanerConfig(int numThreads,
                         long dedupeBufferSize,
                         double dedupeBufferLoadFactor,
                         int ioBufferSize,
                         int maxMessageSize,
                         double maxIoBytesPerSecond,
                         long backoffMs,
                         boolean enableCleaner) {
        this.numThreads = numThreads;
        this.dedupeBufferSize = dedupeBufferSize;
        this.dedupeBufferLoadFactor = dedupeBufferLoadFactor;
        this.ioBufferSize = ioBufferSize;
        this.maxMessageSize = maxMessageSize;
        this.maxIoBytesPerSecond = maxIoBytesPerSecond;
        this.backoffMs = backoffMs;
        this.enableCleaner = enableCleaner;
    }

    public String hashAlgorithm() {
        return HASH_ALGORITHM;
    }
}
