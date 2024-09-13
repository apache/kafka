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
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;

/**
 * {@code CreateFetchRequestsEvent} signals that the {@link Consumer} wants to issue fetch requests to the nodes
 * for the partitions to which the consumer is currently subscribed. The event is completed when the
 * {@link FetchRequestManager} has finished <em>creating</em> (i.e. not enqueuing, sending, or receiving)
 * fetch requests (if any) to send to the broker nodes.
 */
public class CreateFetchRequestsEvent extends CompletableApplicationEvent<Void> {

    public CreateFetchRequestsEvent(final long deadlineMs) {
        super(Type.CREATE_FETCH_REQUESTS, deadlineMs);
    }
}
