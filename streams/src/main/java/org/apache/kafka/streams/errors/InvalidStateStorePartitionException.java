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
package org.apache.kafka.streams.errors;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Indicates that the specific state store being queried via
 * {@link org.apache.kafka.streams.StoreQueryParameters} used a partitioning that is not assigned to this instance.
 * You can use {@link KafkaStreams#allMetadata()} to discover the correct instance that hosts the requested partition.
 */
public class InvalidStateStorePartitionException extends InvalidStateStoreException {

    private static final long serialVersionUID = 1L;

    public InvalidStateStorePartitionException(final String message) {
        super(message);
    }

    public InvalidStateStorePartitionException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

}
