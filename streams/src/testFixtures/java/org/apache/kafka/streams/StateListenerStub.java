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
package org.apache.kafka.streams;

import java.util.HashMap;
import java.util.Map;

public class StateListenerStub implements KafkaStreams.StateListener {
    public int numChanges = 0;
    public KafkaStreams.State oldState;
    public KafkaStreams.State newState;
    public Map<KafkaStreams.State, Long> mapStates = new HashMap<>();

    @Override
    public void onChange(final KafkaStreams.State newState,
                         final KafkaStreams.State oldState) {
        final long prevCount = mapStates.containsKey(newState) ? mapStates.get(newState) : 0;
        numChanges++;
        this.oldState = oldState;
        this.newState = newState;
        mapStates.put(newState, prevCount + 1);
    }
}
