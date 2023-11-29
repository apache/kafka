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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer;

/**
 * This event serves as a mechanism to block the application thread until the consumer has joined the consumer group.
 * It is used to mimic the blocking behavior of the {@link LegacyKafkaConsumer existing Consumer}. The application
 * thread submits this event and then can block on the {@link #future()} to wait for the result.
 */
public class WaitForJoinGroupApplicationEvent extends CompletableApplicationEvent<Void> {

    public WaitForJoinGroupApplicationEvent() {
        super(Type.WAIT_FOR_JOIN_GROUP);
    }
}

