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
package org.apache.kafka.streams.integration.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * A {@link StateListener} that holds zero or more listeners internally and invokes all of them
 * when a state transition occurs (i.e. {@link #onChange(State, State)} is called). If any listener
 * throws {@link RuntimeException} or {@link Error} this immediately stops execution of listeners
 * and causes the thrown exception to be raised.
 */
public class CompositeStateListener implements StateListener {
    private final List<StateListener> listeners;

    public CompositeStateListener(final StateListener... listeners) {
        this(Arrays.asList(listeners));
    }

    public CompositeStateListener(final Collection<StateListener> stateListeners) {
        this.listeners = Collections.unmodifiableList(new ArrayList<>(stateListeners));
    }

    @Override
    public void onChange(final State newState, final State oldState) {
        for (final StateListener listener : listeners) {
            listener.onChange(newState, oldState);
        }
    }
}
