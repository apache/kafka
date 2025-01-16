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
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;

import java.time.Duration;

/**
 * When the user calls {@link Consumer#close()}, this event is sent to signal the {@link ConsumerMembershipManager}
 * to perform the necessary steps to leave the consumer group cleanly, if possible. The event's timeout is based on
 * either the user-provided value to {@link Consumer#close(Duration)} or
 * {@link ConsumerUtils#DEFAULT_CLOSE_TIMEOUT_MS} if {@link Consumer#close()} was called. The event is considered
 * complete when the membership manager receives the heartbeat response that it has left the group.
 */
public class LeaveGroupOnCloseEvent extends CompletableApplicationEvent<Void> {

    public LeaveGroupOnCloseEvent(final long deadlineMs) {
        super(Type.LEAVE_GROUP_ON_CLOSE, deadlineMs);
    }
}
