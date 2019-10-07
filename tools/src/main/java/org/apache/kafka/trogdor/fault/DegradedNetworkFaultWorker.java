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
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Uses the linux utility <pre>tc</pre> (traffic controller) to simulate latency on a specified network device
 */
public class DegradedNetworkFaultWorker implements TaskWorker {

    private static final Logger log = LoggerFactory.getLogger(DegradedNetworkFaultWorker.class);

    private final String id;
    private final Map<String, DegradedNetworkFaultSpec.NodeDegradeSpec> nodeSpecs;
    private WorkerStatusTracker status;

    public DegradedNetworkFaultWorker(String id, Map<String, DegradedNetworkFaultSpec.NodeDegradeSpec> nodeSpecs) {
        this.id = id;
        this.nodeSpecs = nodeSpecs;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status, KafkaFutureImpl<String> haltFuture) throws Exception {
        log.info("Activating DegradedNetworkFaultWorker {}.", id);
        this.status = status;
        this.status.update(new TextNode("enabling traffic control " + id));
        Node curNode = platform.curNode();
        DegradedNetworkFaultSpec.NodeDegradeSpec nodeSpec = nodeSpecs.get(curNode.name());
        if (nodeSpec != null) {
            for (String device : devicesForSpec(nodeSpec)) {
                if (nodeSpec.latencyMs() < 0) {
                    throw new RuntimeException("Expected a positive value for latencyMs, but got " + nodeSpec.latencyMs());
                } else {
                    enableTrafficControl(platform, device, nodeSpec.latencyMs(), nodeSpec.rateLimitKbit());
                }
            }
        }
        this.status.update(new TextNode("enabled traffic control " + id));
    }

    @Override
    public void stop(Platform platform) throws Exception {
        log.info("Deactivating DegradedNetworkFaultWorker {}.", id);
        this.status.update(new TextNode("disabling traffic control " + id));
        Node curNode = platform.curNode();
        DegradedNetworkFaultSpec.NodeDegradeSpec nodeSpec = nodeSpecs.get(curNode.name());
        if (nodeSpec != null) {
            for (String device : devicesForSpec(nodeSpec)) {
                disableTrafficControl(platform, device);
            }
        }
        this.status.update(new TextNode("disabled traffic control " + id));
    }

    private Set<String> devicesForSpec(DegradedNetworkFaultSpec.NodeDegradeSpec nodeSpec) throws Exception {
        Set<String> devices = new HashSet<>();
        if (nodeSpec.networkDevice().isEmpty()) {
            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!networkInterface.isLoopback()) {
                    devices.add(networkInterface.getName());
                }
            }
        } else {
            devices.add(nodeSpec.networkDevice());
        }
        return devices;
    }

    private void enableTrafficControl(Platform platform, String networkDevice, int delayMs, int rateLimitKbps) throws IOException {
        if (delayMs > 0) {
            int deviationMs = Math.max(1, (int) Math.sqrt(delayMs));
            List<String> delay = new ArrayList<>();
            rootHandler(networkDevice, delay::add);
            netemDelay(delayMs, deviationMs, delay::add);
            platform.runCommand(delay.toArray(new String[]{}));

            if (rateLimitKbps > 0) {
                List<String> rate = new ArrayList<>();
                childHandler(networkDevice, rate::add);
                tbfRate(rateLimitKbps, rate::add);
                platform.runCommand(rate.toArray(new String[]{}));
            }
        } else if (rateLimitKbps > 0) {
            List<String> rate = new ArrayList<>();
            rootHandler(networkDevice, rate::add);
            tbfRate(rateLimitKbps, rate::add);
            platform.runCommand(rate.toArray(new String[]{}));
        } else {
            // nothing to do!
        }
    }

    private void rootHandler(String networkDevice, Consumer<String> consumer) {
        Stream.of("sudo", "tc", "qdisc", "add", "dev", networkDevice, "root", "handle", "1:0").forEach(consumer);
    }

    private void childHandler(String networkDevice, Consumer<String> consumer) {
        Stream.of("sudo", "tc", "qdisc", "add", "dev", networkDevice, "parent", "1:1", "handle", "10:").forEach(consumer);
    }

    private void netemDelay(int delayMs, int deviationMs, Consumer<String> consumer) {
        Stream.of("netem", "delay", String.format("%dms", delayMs), String.format("%dms", deviationMs),
                "distribution", "paretonormal").forEach(consumer);
    }

    private void tbfRate(int rateLimitKbit, Consumer<String> consumer) {
        Stream.of("tbf", "rate", String.format("%dkbit", rateLimitKbit), "buffer", "32kbit", "latency", "500").forEach(consumer);
    }

    private void disableTrafficControl(Platform platform, String networkDevice) throws IOException {
        platform.runCommand(new String[] {
            "sudo", "tc", "qdisc", "del", "dev", networkDevice, "root"
        });
    }
}
