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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;

import static java.lang.String.format;

public class EosUtils {

    static void initializeTransactions(final Producer<byte[], byte[]> producer,
                                       final Consumer<byte[], byte[]> consumer,
                                       final String id,
                                       final Logger log,
                                       final String logPrefix) {
        try {
            producer.initTransactions(consumer);
        } catch (final TimeoutException retriable) {
            log.error(
                "Timeout exception caught when initializing transactions for task {}. " +
                    "This might happen if the broker is slow to respond, if the network connection to " +
                    "the broker was interrupted, or if similar circumstances arise. " +
                    "You can increase producer parameter `max.block.ms` to increase this timeout.",
                id,
                retriable
            );
            throw new StreamsException(
                format("%sFailed to initialize task %s due to timeout.", logPrefix, id),
                retriable
            );
        }
    }
}
