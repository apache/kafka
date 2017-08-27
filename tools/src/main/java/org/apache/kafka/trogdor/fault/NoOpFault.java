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

package org.apache.kafka.trogdor.fault;

import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class NoOpFault implements Fault {
    private static final Logger log = LoggerFactory.getLogger(NoOpFault.class);

    private final String id;
    private final FaultSpec spec;

    public NoOpFault(String id, FaultSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public FaultSpec spec() {
        return spec;
    }

    @Override
    public void activate(Platform platform) {
        log.info("Activating NoOpFault...");
    }

    @Override
    public void deactivate(Platform platform) {
        log.info("Deactivating NoOpFault...");
    }

    @Override
    public Set<String> targetNodes(Topology topology) {
        Set<String> set = new HashSet<>();
        for (Map.Entry<String, Node> entry : topology.nodes().entrySet()) {
            if (Node.Util.getTrogdorAgentPort(entry.getValue()) > 0) {
                set.add(entry.getKey());
            }
        }
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NoOpFault that = (NoOpFault) o;
        return Objects.equals(id, that.id) &&
            Objects.equals(spec, that.spec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, spec);
    }

    @Override
    public String toString() {
        return "NoOpFault(id=" + id + ", spec=" + JsonUtil.toJsonString(spec) + ")";
    }
}
