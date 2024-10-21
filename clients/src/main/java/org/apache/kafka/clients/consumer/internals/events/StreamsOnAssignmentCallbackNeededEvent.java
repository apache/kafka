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

import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface;

import java.util.Objects;

public class StreamsOnAssignmentCallbackNeededEvent extends CompletableBackgroundEvent<Void> {

    private final StreamsAssignmentInterface.Assignment assignment;

    public StreamsOnAssignmentCallbackNeededEvent(StreamsAssignmentInterface.Assignment assignment) {
        super(Type.STREAMS_ON_ASSIGNMENT_CALLBACK_NEEDED, Long.MAX_VALUE);
        this.assignment = Objects.requireNonNull(assignment);
    }

    public StreamsAssignmentInterface.Assignment assignment() {
        return assignment;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
            ", assignment=" + assignment;
    }
}

