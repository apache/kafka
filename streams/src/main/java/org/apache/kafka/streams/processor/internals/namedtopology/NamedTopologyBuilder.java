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

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Properties;

@Deprecated
public class NamedTopologyBuilder extends StreamsBuilder {

    NamedTopologyBuilder(final String topologyName, final StreamsConfig applicationConfigs, final Properties topologyOverrides) {
        super(new TopologyConfig(topologyName, applicationConfigs, topologyOverrides));
        internalTopologyBuilder.setNamedTopology((NamedTopology) topology);
    }

    @Override
    public synchronized NamedTopology build() {
        super.build(internalTopologyBuilder.topologyConfigs().topologyOverrides);
        return (NamedTopology) topology;
    }

    @Override
    protected NamedTopology newTopology(final TopologyConfig topologyConfigs) {
        return new NamedTopology(new InternalTopologyBuilder(topologyConfigs));
    }
}
