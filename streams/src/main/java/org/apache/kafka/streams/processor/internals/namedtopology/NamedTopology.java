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

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NamedTopology extends Topology {

    private final Logger log = LoggerFactory.getLogger(NamedTopology.class);
    private String name;
    
    void setTopologyName(final String newTopologyName) {
        if (name != null) {
            log.error("Unable to set topologyName = {} because the name is already set to {}", newTopologyName, name);
            throw new IllegalStateException("Tried to set topologyName but the name was already set");
        }
        name = newTopologyName;
        internalTopologyBuilder.setNamedTopology(this);
    }

    public String name() {
        return name;
    }

    public List<String> sourceTopics() {
        return super.internalTopologyBuilder.fullSourceTopicNames();
    }

    InternalTopologyBuilder internalTopologyBuilder() {
        return internalTopologyBuilder;
    }
}
