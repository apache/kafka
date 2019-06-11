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

import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Uses the linux utility <pre>tc</pre> (traffic controller) to simulate latency on a specified network device
 */
public class NetworkDegradeFaultWorker implements TaskWorker {

    private static final Logger log = LoggerFactory.getLogger(NetworkDegradeFaultWorker.class);

    private final String id;
    private final Map<String, NetworkDegradeFaultSpec.NodeDegradeSpec> nodeSpecs;
    private WorkerStatusTracker status;

    public NetworkDegradeFaultWorker(String id, Map<String, NetworkDegradeFaultSpec.NodeDegradeSpec> nodeSpecs) {
        this.id = id;
        this.nodeSpecs = nodeSpecs;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status, KafkaFutureImpl<String> haltFuture) throws Exception {
        log.info("Activating NetworkDegradeFaultWorker {}.", id);
        this.status = status;
        this.status.update(new TextNode("enabling traffic control " + id));
        Node curNode = platform.curNode();
        NetworkDegradeFaultSpec.NodeDegradeSpec nodeSpec = nodeSpecs.get(curNode.name());
        if (nodeSpec != null) {
            enableTrafficControl(platform, nodeSpec.getNetworkDevice(), nodeSpec.getLatency());
        }
        this.status.update(new TextNode("enabled traffic control " + id));
    }

    @Override
    public void stop(Platform platform) throws Exception {
        log.info("Deactivating NetworkDegradeFaultWorker {}.", id);
        this.status.update(new TextNode("disabling traffic control " + id));
        Node curNode = platform.curNode();
        NetworkDegradeFaultSpec.NodeDegradeSpec nodeSpec = nodeSpecs.get(curNode.name());
        if (nodeSpec != null) {
            disableTrafficControl(platform, nodeSpec.getNetworkDevice());
        }
        this.status.update(new TextNode("disabled traffic control " + id));
    }

    private void enableTrafficControl(Platform platform, String networkDevice, int delayMs) throws IOException {
        int deviationMs = Math.max(1, (int) Math.sqrt(delayMs));
        platform.runCommand(new String[] {
            "sudo", "tc", "qdisc", "add", "dev", networkDevice, "root", "netem",
            "delay", String.format("%dms", delayMs), String.format("%dms", deviationMs), "distribution", "normal"
        });
    }

    private void disableTrafficControl(Platform platform, String networkDevice) throws IOException {
        platform.runCommand(new String[] {
            "sudo", "tc", "qdisc", "del", "dev", networkDevice, "root"
        });
    }
}
