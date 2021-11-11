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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

/**
 * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
 * notifies producer interceptors about the request completion.
 */
public class InterceptorCallback<K, V> implements Callback {
    private final Callback userCallback;
    private final ProducerInterceptors<K, V> interceptors;
    private final TopicPartition tp;

    public InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
        this.userCallback = userCallback;
        this.interceptors = interceptors;
        this.tp = tp;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
        this.interceptors.onAcknowledgement(metadata, exception);
        if (this.userCallback != null)
            this.userCallback.onCompletion(metadata, exception);
    }
}
