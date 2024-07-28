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
package org.apache.kafka.streams.errors.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.processor.TaskId;

/**
 * Default implementation of {@link ErrorHandlerContext} that provides access to the metadata of the record that caused the error.
 */
public class DefaultErrorHandlerContext implements ErrorHandlerContext {
    private final String topic;
    private final int partition;
    private final long offset;
    private final Headers headers;
    private final byte[] sourceRawKey;
    private final byte[] sourceRawValue;
    private final String processorNodeId;
    private final TaskId taskId;

    public DefaultErrorHandlerContext(final String topic,
                                      final int partition,
                                      final long offset,
                                      final Headers headers,
                                      final byte[] sourceRawKey,
                                      final byte[] sourceRawValue,
                                      final String processorNodeId,
                                      final TaskId taskId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.headers = headers;
        this.sourceRawKey = sourceRawKey;
        this.sourceRawValue = sourceRawValue;
        this.processorNodeId = processorNodeId;
        this.taskId = taskId;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public byte[] sourceRawKey() {
        return sourceRawKey;
    }

    @Override
    public byte[] sourceRawValue() {
        return sourceRawValue;
    }

    @Override
    public String processorNodeId() {
        return processorNodeId;
    }

    @Override
    public TaskId taskId() {
        return taskId;
    }
}
