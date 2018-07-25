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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;

import java.util.Map;

public class RequestInformation {
    public final Duration timeout;
    public final RebalanceKafkaConsumer.ConsumerRequest request;
    public final Object inputArgument;
    public final TaskCompletionCallback callback;

    public final Map<TopicPartition, OffsetAndMetadata> offsets;
    public final long hashCode1;
    public final long hashCode2;

    public RequestInformation(Duration timeout, RebalanceKafkaConsumer.ConsumerRequest request, Object inputArgument, TaskCompletionCallback callback) {
        this(timeout, request, inputArgument, callback, null, 0, 0);
    }

    public RequestInformation(Duration timeout,
                              RebalanceKafkaConsumer.ConsumerRequest request,
                              Object inputArgument,
                              TaskCompletionCallback callback,
                              Map<TopicPartition, OffsetAndMetadata> offsets,
                              long hashCode1,
                              long hashCode2) {
        this.timeout = timeout;
        this.request = request;
        this.inputArgument = inputArgument;
        this.callback = callback;
        this.offsets = offsets;
        this.hashCode1 = hashCode1;
        this.hashCode2 = hashCode2;
    }
}
