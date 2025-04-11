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
package org.apache.kafka.streams.integration;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *  This class allows to access the {@link KafkaStreams} a {@link StreamThread.StateListener} object.
 *
 */
public class KafkaStreamsWrapper extends KafkaStreams {

    public KafkaStreamsWrapper(final Topology topology,
                               final Properties props) {
        super(topology, props);
    }

    public List<StreamThread> streamThreads() {
        return new ArrayList<>(threads);
    }

    /**
     * An app can set a single {@link StreamThread.StateListener} so that the app is notified when state changes.
     *
     * @param listener a new StreamThread state listener
     * @throws IllegalStateException if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
     */
    public void setStreamThreadStateListener(final StreamThread.StateListener listener) {
        if (state == State.CREATED) {
            for (final StreamThread thread : threads) {
                thread.setStateListener(listener);
            }
        } else {
            throw new IllegalStateException("Can only set StateListener in CREATED state. " +
                "Current state is: " + state);
        }
    }
}
