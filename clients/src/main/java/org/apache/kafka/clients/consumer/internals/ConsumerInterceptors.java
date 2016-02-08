/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;


import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * A container that holds the list {@link org.apache.kafka.clients.consumer.ConsumerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 */
public class ConsumerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerInterceptors.class);
    private final List<ConsumerInterceptor<K, V>> interceptors;

    public ConsumerInterceptors(List<ConsumerInterceptor<K, V>> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * This is called when the records are about to be returned to the user.
     * <p>
     * This method calls {@link ConsumerInterceptor#onConsume(ConsumerRecords)} for each
     * interceptor. Records returned from each interceptor get passed to onConsume() of the next interceptor
     * in the chain of interceptors.
     * <p>
     * This method does not throw exceptions. If any of the interceptors in the chain throws an exception,
     * it gets caught and logged, and next interceptor in the chain is called with 'records' returned by the
     * previous successful interceptor onConsume call.
     *
     * @param records records to be consumed by the client.
     * @return records that are either modified by interceptors or same as records passed to this method.
     */
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        ConsumerRecords<K, V> interceptRecords = records;
        for (ConsumerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptRecords = interceptor.onConsume(interceptRecords);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors
                log.warn("Error executing interceptor onConsume callback", e);
            }
        }
        return interceptRecords;
    }

    /**
     * This is called when commit request returns successfully from the broker.
     * <p>
     * This method calls {@link ConsumerInterceptor#onCommit(Map)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of the interceptors in the chain are logged, but not propagated.
     *
     * @param offsets A map of offsets by partition with associated metadata
     */
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (ConsumerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.onCommit(offsets);
            } catch (Exception e) {
                // do not propagate interceptor exception, just log
                log.warn("Error executing interceptor onCommit callback", e);
            }
        }
    }

    /**
     * Closes every interceptor in a container.
     */
    @Override
    public void close() {
        for (ConsumerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.close();
            } catch (Exception e) {
                log.error("Failed to close consumer interceptor ", e);
            }
        }
    }
}
