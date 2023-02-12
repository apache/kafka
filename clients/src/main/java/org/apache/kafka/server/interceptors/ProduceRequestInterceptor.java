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

package org.apache.kafka.server.interceptors;

import org.apache.kafka.common.header.Header;

import java.util.regex.Pattern;

/**
 * ProduceRequestInterceptors can be defined to perform custom, light-weight processing on every record received by a
 * broker.
 *
 * Broker-side interceptors should be used with caution:
 *  1. Processing messages that were sent in compressed format by the producer will need to be decompressed and then
 *    re-compressed to perform broker-side processing
 *  2. Performing unduly long or complex computations can negatively impact overall cluster health and performance
 *
 * Potential use cases:
 *   - Schema validation
 *   - Privacy enforcement
 *   - Decoupling server-side and client-side serialization
 */
public abstract class ProduceRequestInterceptor {
    // Custom function for mutating the original message. If the method returns a ProduceRequestInterceptorSkipRecordException,
    // the record will be removed from the batch and won't be persisted in the target log. All other exceptions are
    // considered "fatal" and will result in a request error
    public abstract ProduceRequestInterceptorResult processRecord(byte[] key, byte[] value, String topic, int partition, Header[] headers) throws Exception;

    // Define the topic name pattern that will determine whether this interceptor runs on a given batch of records
    public abstract Pattern interceptorTopicPattern();

    // Method that gets called during the interceptor's initialization to configure itself
    public abstract void configure();
}
