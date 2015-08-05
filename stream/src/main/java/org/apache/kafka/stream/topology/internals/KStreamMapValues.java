/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.stream.topology.internals;

import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.ValueMapper;

class KStreamMapValues<K, V, V1> extends KStreamImpl<K, V> {

    private final ValueMapper<V, V1> mapper;

    KStreamMapValues(ValueMapper<V, V1> mapper, KStreamTopology topology) {
        super(topology);
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void receive(Object key, Object value, long timestamp) {
        synchronized (this) {
            V newValue = mapper.apply((V1) value);
            forward(key, newValue, timestamp);
        }
    }

}
