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

import org.apache.kafka.common.utils.Timer;

/**
 * Application event triggered when a user calls the unsubscribe API. This will make the consumer
 * release all its assignments and send a heartbeat request to leave the consumer group.
 * This event holds a future that will complete when the invocation of callbacks to release
 * complete and the heartbeat to leave the group is sent out (minimal effort to send the
 * leave group heartbeat, without waiting for any response or considering timeouts).
 */
public class UnsubscribeEvent extends CompletableApplicationEvent<Void> {

    public UnsubscribeEvent(final Timer timer) {
        super(Type.UNSUBSCRIBE, timer);
    }
}

