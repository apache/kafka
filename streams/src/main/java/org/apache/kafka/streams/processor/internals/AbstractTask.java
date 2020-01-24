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
package org.apache.kafka.streams.processor.internals;

import static org.apache.kafka.streams.processor.internals.Task.State.CLOSED;
import static org.apache.kafka.streams.processor.internals.Task.State.CLOSING;
import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;
import static org.apache.kafka.streams.processor.internals.Task.State.RESTORING;
import static org.apache.kafka.streams.processor.internals.Task.State.RUNNING;
import static org.apache.kafka.streams.processor.internals.Task.State.SUSPENDED;

public abstract class AbstractTask implements Task {
    private Task.State state = CREATED;

    @Override
    public final Task.State state() {
        return state;
    }

    final void transitionTo(final Task.State newState) {
        final State oldState = state();
        if (oldState == CREATED && (newState == RESTORING || newState == CLOSING)) {
        } else if (oldState == RESTORING && (newState == RUNNING || newState == SUSPENDED || newState == CLOSING)) {
        } else if (oldState == RUNNING && (newState == SUSPENDED || newState == CLOSING)) {
        } else if (oldState == SUSPENDED && (newState == RUNNING || newState == CLOSING)) {
        } else if (oldState == CLOSING && (newState == CLOSING || newState== CLOSED)) {
        } else {
            throw new IllegalStateException("Invalid transition from " + oldState + " to " + newState);
        }
        state = newState;
    }
}
