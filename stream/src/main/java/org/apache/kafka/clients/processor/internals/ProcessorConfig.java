/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.processor.internals;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.stream.StreamingConfig;

import java.io.File;
import java.util.Properties;

public class ProcessorConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef().define(StreamingConfig.TOPICS_CONFIG,
            Type.STRING,
            "",
            Importance.HIGH,
            "All the possible topic names this job need to interact with")
            .define(StreamingConfig.STATE_DIR_CONFIG,
                Type.STRING,
                System.getProperty("java.io.tmpdir"),
                Importance.MEDIUM,
                "")
            .define(StreamingConfig.POLL_TIME_MS_CONFIG,
                Type.LONG,
                100,
                Importance.LOW,
                "The amount of time to block waiting for input.")
            .define(StreamingConfig.COMMIT_TIME_MS_CONFIG,
                Type.LONG,
                30000,
                Importance.HIGH,
                "The frequency with which to save the position of the processor.")
            .define(StreamingConfig.WINDOW_TIME_MS_CONFIG,
                Type.LONG,
                -1L,
                Importance.MEDIUM,
                "Setting this to a non-negative value will cause the processor to get called with this frequency even if there is no message.")
            .define(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG,
                Type.INT,
                1000,
                Importance.LOW,
                "The maximum number of records to buffer per partition")
            .define(StreamingConfig.STATE_CLEANUP_DELAY_CONFIG,
                Type.LONG,
                60000,
                Importance.LOW,
                "The amount of time to wait before deleting state when a partition has migrated.")
            .define(StreamingConfig.TOTAL_RECORDS_TO_PROCESS,
                Type.LONG,
                -1L,
                Importance.LOW,
                "Exit after processing this many records.")
            .define(StreamingConfig.NUM_STREAM_THREADS,
                Type.INT,
                1,
                Importance.LOW,
                "The number of threads to execute stream processing.");
    }

    public final String topics;
    public final File stateDir;
    public final long pollTimeMs;
    public final long commitTimeMs;
    public final long windowTimeMs;
    public final int bufferedRecordsPerPartition;
    public final long stateCleanupDelay;
    public final long totalRecordsToProcess;
    public final int numStreamThreads;

    public ProcessorConfig(Properties processor) {
        super(CONFIG, processor);
        this.topics = this.getString(StreamingConfig.TOPICS_CONFIG);
        this.stateDir = new File(this.getString(StreamingConfig.STATE_DIR_CONFIG));
        this.pollTimeMs = this.getLong(StreamingConfig.POLL_TIME_MS_CONFIG);
        this.commitTimeMs = this.getLong(StreamingConfig.COMMIT_TIME_MS_CONFIG);
        this.windowTimeMs = this.getLong(StreamingConfig.WINDOW_TIME_MS_CONFIG);
        this.bufferedRecordsPerPartition = this.getInt(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        this.stateCleanupDelay = this.getLong(StreamingConfig.STATE_CLEANUP_DELAY_CONFIG);
        this.totalRecordsToProcess = this.getLong(StreamingConfig.TOTAL_RECORDS_TO_PROCESS);
        this.numStreamThreads = this.getInt(StreamingConfig.NUM_STREAM_THREADS);
    }

}
