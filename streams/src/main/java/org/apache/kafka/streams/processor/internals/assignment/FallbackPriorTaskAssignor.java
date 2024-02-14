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

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A special task assignor implementation to be used as a fallback in case the
 * configured assignor couldn't be invoked.
 *
 * Specifically, this assignor must:
 * 1. ignore the task lags in the ClientState map
 * 2. always return true, indicating that a follow-up rebalance is needed
 */
public class FallbackPriorTaskAssignor implements TaskAssignor {
    private final StickyTaskAssignor delegate;

    public FallbackPriorTaskAssignor() {
        delegate = new StickyTaskAssignor(true);
    }

    @Override
    public boolean assign(final Map<UUID, ClientState> clients,
                          final Set<TaskId> allTaskIds,
                          final Set<TaskId> statefulTaskIds,
                          final RackAwareTaskAssignor rackAwareTaskAssignor,
                          final AssignmentConfigs configs) {
        // Pass null for RackAwareTaskAssignor to disable it if we fallback
        delegate.assign(clients, allTaskIds, statefulTaskIds, null, configs);
        return true;
    }
}
