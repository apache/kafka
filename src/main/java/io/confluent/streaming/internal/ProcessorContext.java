/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.confluent.streaming.internal;

import io.confluent.streaming.StorageEngine;
import io.confluent.streaming.StreamingConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A collection of values and helper methods available to the {@link StreamProcessor} during initialization.
 */
public class ProcessorContext {

    private final int id;
    private final File stateDir;
    private final StreamingConfig config;
    private final Map<String, StorageEngine> stores;
    private final Metrics metrics;

    public ProcessorContext(int id,
                            StreamingConfig config,
                            File stateDir,
                            Metrics metrics) {
        this.id = id;
        this.config = config;
        this.stateDir = stateDir;
        this.stores = new HashMap<String, StorageEngine>();
        this.metrics = metrics;
    }

    /**
     * The id of this {@link StreamProcessor}
     */
    public int id() {
        return this.id;
    }

    /**
     * The streaming config provided for this job
     */
    public StreamingConfig config() {
        return this.config;
    }

    /**
     * The directory allocated to this processor for maintaining any local state
     */
    public File stateDir() {
        return this.stateDir;
    }

    /**
     * Get the metrics associated with this processor instance
     */
    public Metrics metrics() {
        return metrics;
    }

}
