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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.PollEvent;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is constructed from within the {@link SharedConsumerState} instance, which means it's available
 * for both the application and network threads to use. The main user is the {@link AbstractMembershipManager} for
 * mutations and the {@link SharedConsumerState#canSkipWaitingOnPoll(long)} method for determining if the costly
 * {@link PollEvent} can be sent in the background or not.
 *
 * <p/>
 *
 * Yes, this class is a wrapper around a simple {@link AtomicBoolean}, but the intention behind dedicating a class
 * to it hopefully makes the shared nature and its purpose more apparent.
 */
public class SharedReconciliationState {

    private final AtomicBoolean value;

    public SharedReconciliationState() {
        this(false);
    }

    public SharedReconciliationState(boolean value) {
        this.value = new AtomicBoolean(value);
    }

    public boolean isInProgress() {
        return value.get();
    }

    public void setInProgress(boolean value) {
        this.value.set(value);
    }
}
