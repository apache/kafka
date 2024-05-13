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
package org.apache.kafka.streams.processor.internals.assignment;

import java.time.Instant;
import java.util.Set;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.ProcessId;

public class KafkaStreamsAssignmentImpl implements KafkaStreamsAssignment {

    private final ProcessId processId;
    private final Set<AssignedTask> assignment;
    private final Instant followupRebalanceDeadline;

    public KafkaStreamsAssignmentImpl(final ProcessId processId,
                                      final Set<AssignedTask> assignment,
                                      final Instant followupRebalanceDeadline) {
        this.processId = processId;
        this.assignment = assignment;
        this.followupRebalanceDeadline = followupRebalanceDeadline;
    }

    @Override
    public ProcessId processId() {
        return processId;
    }

    @Override
    public Set<AssignedTask> assignment() {
        return assignment;
    }

    @Override
    public Instant followupRebalanceDeadline() {
        return followupRebalanceDeadline;
    }
}
