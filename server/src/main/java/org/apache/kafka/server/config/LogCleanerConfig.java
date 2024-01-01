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
package org.apache.kafka.server.config;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.CleanerConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LogCleanerConfig {
    public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(
        KafkaConfig.LOG_CLEANER_THREADS_PROP,
        KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP,
        KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP,
        KafkaConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP,
        KafkaConfig.MESSAGE_MAX_BYTES_PROP,
        KafkaConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP,
        KafkaConfig.LOG_CLEANER_BACKOFF_MS_PROP
    );

    public static CleanerConfig cleanerConfig(KafkaConfig config) {
        return new CleanerConfig(config.logCleanerThreads(),
                config.logCleanerDedupeBufferSize(),
                config.logCleanerDedupeBufferLoadFactor(),
                config.logCleanerIoBufferSize(),
                config.messageMaxBytes(),
                config.logCleanerIoMaxBytesPerSecond(),
                config.logCleanerBackoffMs(),
                config.logCleanerEnable());
    }
    public static final String MAX_BUFFER_UTILIZATION_PERCENT_METRIC_NAME = "max-buffer-utilization-percent";
    public static final String CLEANER_RECOPY_PERCENT_METRIC_NAME = "cleaner-recopy-percent";
    public static final String MAX_CLEAN_TIME_METRIC_NAME = "max-clean-time-secs";
    public static final String MAX_COMPACTION_DELAY_METRICS_NAME = "max-compaction-delay-secs";
    public static final String DEAD_THREAD_COUNT_METRIC_NAME = "DeadThreadCount";

    // Package private for testing
    public static final Set<String> METRIC_NAMES = new HashSet<>(Arrays.asList(
            MAX_BUFFER_UTILIZATION_PERCENT_METRIC_NAME,
            CLEANER_RECOPY_PERCENT_METRIC_NAME,
            MAX_CLEAN_TIME_METRIC_NAME,
            MAX_COMPACTION_DELAY_METRICS_NAME,
            DEAD_THREAD_COUNT_METRIC_NAME
    ));

}
