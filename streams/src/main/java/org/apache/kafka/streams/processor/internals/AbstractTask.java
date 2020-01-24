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

public abstract class AbstractTask implements Task {
    private Task.State state = Task.State.CREATED;

    @Override
    public final Task.State state() {
        return state;
    }

    final void transitionTo(final Task.State newState) {
        final State oldState = state();
        if (oldState == State.CREATED && (newState == State.RESTORING || newState == State.CLOSED)) {
        } else if (oldState == State.RESTORING && (newState == State.RUNNING || newState == State.SUSPENDED || newState == State.CLOSED)) {
        } else if (oldState == State.RUNNING && (newState == State.SUSPENDED || newState == State.CLOSED)) {
        } else if (oldState == State.SUSPENDED && (newState == State.RUNNING || newState == State.CLOSED)) {
        } else {
            throw new IllegalStateException("Invalid transition from " + oldState + " to " + newState);
        }
        state = newState;
    }
}
