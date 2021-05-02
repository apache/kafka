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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Properties;

public class KafkaStreamsNamedTopologyWrapper extends KafkaStreams {

    //TODO It should be possible to start up streams with no NamedTopology (or regular Topology) at all, in the meantime we can just pass in an empty NamedTopology
    public KafkaStreamsNamedTopologyWrapper(final NamedTopology topology, final Properties props, final KafkaClientSupplier clientSupplier) {
        super(topology, props, clientSupplier);
    }

    public NamedTopology getTopologyByName(final String name) {
        throw new UnsupportedOperationException();
    }

    public void addNamedTopology(final NamedTopology topology) {
        throw new UnsupportedOperationException();
    }

    public void removeNamedTopology(final NamedTopology topology) {
        throw new UnsupportedOperationException();
    }
}
