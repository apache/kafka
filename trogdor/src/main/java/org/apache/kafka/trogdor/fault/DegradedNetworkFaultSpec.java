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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.Collections;
import java.util.Map;

public class DegradedNetworkFaultSpec extends TaskSpec {

    public static class NodeDegradeSpec {
        private final String networkDevice;
        private final int latencyMs;
        private final int rateLimitKbit;

        public NodeDegradeSpec(
                @JsonProperty("networkDevice") String networkDevice,
                @JsonProperty("latencyMs") Integer latencyMs,
                @JsonProperty("rateLimitKbit") Integer rateLimitKbit) {
            this.networkDevice = networkDevice == null ? "" : networkDevice;
            this.latencyMs = latencyMs == null ? 0 : latencyMs;
            this.rateLimitKbit = rateLimitKbit == null ? 0 : rateLimitKbit;
        }

        @JsonProperty("networkDevice")
        public String networkDevice() {
            return networkDevice;
        }

        @JsonProperty("latencyMs")
        public int latencyMs() {
            return latencyMs;
        }

        @JsonProperty("rateLimitKbit")
        public int rateLimitKbit() {
            return rateLimitKbit;
        }

        @Override
        public String toString() {
            return "NodeDegradeSpec{" +
                    "networkDevice='" + networkDevice + '\'' +
                    ", latencyMs=" + latencyMs +
                    ", rateLimitKbit=" + rateLimitKbit +
                    '}';
        }
    }

    private final Map<String, NodeDegradeSpec> nodeSpecs;

    @JsonCreator
    public DegradedNetworkFaultSpec(@JsonProperty("startMs") long startMs,
                                    @JsonProperty("durationMs") long durationMs,
                                    @JsonProperty("nodeSpecs") Map<String, NodeDegradeSpec> nodeSpecs) {
        super(startMs, durationMs);
        this.nodeSpecs = nodeSpecs == null ? Collections.emptyMap() : Collections.unmodifiableMap(nodeSpecs);
    }

    @Override
    public TaskController newController(String id) {
        return topology -> nodeSpecs.keySet();
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new DegradedNetworkFaultWorker(id, nodeSpecs);
    }

    @JsonProperty("nodeSpecs")
    public Map<String, NodeDegradeSpec> nodeSpecs() {
        return nodeSpecs;
    }
}
