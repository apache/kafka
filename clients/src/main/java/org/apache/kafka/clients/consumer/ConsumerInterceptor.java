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
package org.apache.kafka.clients.consumer;


import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * A plugin interface to allow things to listen for events happening to the record at different points on the consumer.
 * <p>
 * This class will get consumer config properties via <code>configure()</code> method, including clientId assigned
 * by KafkaConsumer if not specified in the consumer config. The interceptor implementation needs to be aware that it will be
 * sharing consumer config namespace with other interceptors and serializers, and ensure that there are no conflicts.
 * <p>
 * ConsumerInterceptor callbacks are called from the same thread. Since interceptor callbacks are called for every record,
 * the interceptor implementation should be careful about adding performance overhead to consumer.
 */
public interface ConsumerInterceptor<K, V> extends Configurable {

    /**
     * This is called when the records are about to be returned to the user.
     * <p>
     * This method is allowed to mutate consumer records, in which case the new records will be returned.
     * Any exception thrown by this method will be ignored by the caller.
     *
     * @param records records to be consumed by the client.
     * @return records that are either modified by the interceptor or same as records passed to this method.
     */
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records);

    /**
     * This is called when offsets get committed.
     * <p>
     * Any exception thrown by this method will be ignored by the caller.
     *
     * @param offsets A map of offsets by partition with associated metadata
     */
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * This is called when interceptor is closed
     */
    public void close();
}
