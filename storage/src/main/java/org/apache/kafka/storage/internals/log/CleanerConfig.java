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

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.config.ServerTopicConfigSynonyms;

/**
 * Configuration parameters for the log cleaner.
 */
public class CleanerConfig {
    public static final String HASH_ALGORITHM = "MD5";
    public static final int LOG_CLEANER_THREADS = 1;
    public static final double LOG_CLEANER_IO_MAX_BYTES_PER_SECOND = Double.MAX_VALUE;
    public static final long LOG_CLEANER_DEDUPE_BUFFER_SIZE = 128 * 1024 * 1024L;
    public static final int LOG_CLEANER_IO_BUFFER_SIZE = 512 * 1024;
    public static final double LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR = 0.9d;
    public static final int LOG_CLEANER_BACKOFF_MS = 15 * 1000;
    public static final boolean LOG_CLEANER_ENABLE = true;

    public static final String LOG_CLEANER_THREADS_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "threads";
    public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "io.max.bytes.per.second";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "dedupe.buffer.size";
    public static final String LOG_CLEANER_IO_BUFFER_SIZE_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "io.buffer.size";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "io.buffer.load.factor";
    public static final String LOG_CLEANER_BACKOFF_MS_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "backoff.ms";
    public static final String LOG_CLEANER_MIN_CLEAN_RATIO_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG);
    public static final String LOG_CLEANER_ENABLE_PROP = ServerTopicConfigSynonyms.LOG_CLEANER_PREFIX + "enable";
    public static final String LOG_CLEANER_DELETE_RETENTION_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.DELETE_RETENTION_MS_CONFIG);
    public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG);
    public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG);

    public static final String LOG_CLEANER_MIN_CLEAN_RATIO_DOC = "The minimum ratio of dirty log to total log for a log to eligible for cleaning. " +
            "If the " + LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP + " or the " + LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP +
            " configurations are also specified, then the log compactor considers the log eligible for compaction " +
            "as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) " +
            "records for at least the " + LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP + " duration, or (ii) if the log has had " +
            "dirty (uncompacted) records for at most the " + LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP + " period.";
    public static final String LOG_CLEANER_THREADS_DOC = "The number of background threads to use for log cleaning";
    public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_DOC = "The log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE_DOC = "The total memory used for log deduplication across all cleaner threads";
    public static final String LOG_CLEANER_IO_BUFFER_SIZE_DOC = "The total memory used for log cleaner I/O buffers across all cleaner threads";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_DOC = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value " +
            "will allow more log to be cleaned at once but will lead to more hash collisions";
    public static final String LOG_CLEANER_BACKOFF_MS_DOC = "The amount of time to sleep when there are no logs to clean";
    public static final String LOG_CLEANER_ENABLE_DOC = "Enable the log cleaner process to run on the server. Should be enabled if using any topics with a cleanup.policy=compact including the internal offsets topic. If disabled those topics will not be compacted and continually grow in size.";
    public static final String LOG_CLEANER_DELETE_RETENTION_MS_DOC = "The amount of time to retain tombstone message markers for log compacted topics. This setting also gives a bound " +
            "on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise  " +
            "tombstones messages may be collected before a consumer completes their scan).";
    public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS_DOC = "The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted.";
    public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS_DOC = "The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted.";

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
