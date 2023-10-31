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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;

/**
 * This event is sent to the {@link ConsumerNetworkThread consumer's network thread} by the application thread
 * when the user calls the {@link Consumer#groupMetadata()} API. The information for the current state of the
 * group member is managed on the consumer network thread and thus requires this jump between threads :(
 */
public class RetrieveGroupMetadataApplicationEvent extends CompletableApplicationEvent<ConsumerGroupMetadata> {

    public RetrieveGroupMetadataApplicationEvent() {
        super(Type.RETRIEVE_GROUP_METADATA);
    }
}
