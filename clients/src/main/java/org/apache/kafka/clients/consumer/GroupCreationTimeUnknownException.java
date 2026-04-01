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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.KafkaException;

/**
 * Thrown when {@code auto.offset.reset=by_start_time} is configured and the broker returns a
 * group creation timestamp of {@code -1} (unknown). This can happen when:
 * <ul>
 *   <li>The broker does not support KIP-1282 (older broker version).</li>
 *   <li>The group was created before this feature was introduced and the creation time was
 *       never recorded.</li>
 * </ul>
 *
 * <p>This exception is intentionally distinct from {@link NoOffsetForPartitionException}, which
 * signals the absence of a committed offset. {@code GroupCreationTimeUnknownException} signals
 * that the anchor timestamp required by {@code by_start_time} is unavailable, making it
 * impossible to safely determine the correct starting offset without risking data loss.
 *
 * <p>Upon throwing this exception, the consumer does not attempt any automatic fallback. The
 * exception is propagated to the caller's {@code poll()} invocation, leaving the recovery
 * strategy to the user.
 */
public class GroupCreationTimeUnknownException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public GroupCreationTimeUnknownException(String message) {
        super(message);
    }
}
