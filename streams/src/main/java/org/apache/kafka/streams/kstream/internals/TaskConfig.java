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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TaskConfig {
    public final long maxTaskIdleMs;
    public final long taskTimeoutMs;
    public final int maxBufferedSize;
    public final TimestampExtractor timestampExtractor;
    public final DeserializationExceptionHandler deserializationExceptionHandler;
    public final ProcessingExceptionHandler processingExceptionHandler;
    public final boolean eosEnabled;

    public TaskConfig(final long maxTaskIdleMs,
                       final long taskTimeoutMs,
                       final int maxBufferedSize,
                       final TimestampExtractor timestampExtractor,
                       final DeserializationExceptionHandler deserializationExceptionHandler,
                       final ProcessingExceptionHandler processingExceptionHandler,
                       final boolean eosEnabled) {
        this.maxTaskIdleMs = maxTaskIdleMs;
        this.taskTimeoutMs = taskTimeoutMs;
        this.maxBufferedSize = maxBufferedSize;
        this.timestampExtractor = timestampExtractor;
        this.deserializationExceptionHandler = deserializationExceptionHandler;
        this.processingExceptionHandler = processingExceptionHandler;
        this.eosEnabled = eosEnabled;
    }

    public TaskConfig getTaskConfig() {
        return new TaskConfig(
            maxTaskIdleMs,
            taskTimeoutMs,
            maxBufferedSize,
            timestampExtractor,
            deserializationExceptionHandler,
            processingExceptionHandler,
            eosEnabled
        );
    }
}
