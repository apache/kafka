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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ProcessStopFaultWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ProcessStopFaultWorker.class);

    private final String id;

    private final String javaProcessName;

    private WorkerStatusTracker status;

    public ProcessStopFaultWorker(String id, String javaProcessName) {
        this.id = id;
        this.javaProcessName = javaProcessName;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> errorFuture) throws Exception {
        this.status = status;
        log.info("Activating ProcessStopFault {}.", id);
        this.status.update(new TextNode("stopping " + javaProcessName));
        sendSignals(platform, "SIGSTOP");
        this.status.update(new TextNode("stopped " + javaProcessName));
    }

    @Override
    public void stop(Platform platform) throws Exception {
        log.info("Deactivating ProcessStopFault {}.", id);
        this.status.update(new TextNode("resuming " + javaProcessName));
        sendSignals(platform, "SIGCONT");
        this.status.update(new TextNode("resumed " + javaProcessName));
    }

    private void sendSignals(Platform platform, String signalName) throws Exception {
        String jcmdOutput = platform.runCommand(new String[] {"jcmd"});
        String[] lines = jcmdOutput.split("\n");
        List<Integer> pids = new ArrayList<>();
        for (String line : lines) {
            if (line.contains(javaProcessName)) {
                String[] components = line.split(" ");
                try {
                    pids.add(Integer.parseInt(components[0]));
                } catch (NumberFormatException e) {
                    log.error("Failed to parse process ID from line {}", e);
                }
            }
        }
        if (pids.isEmpty()) {
            log.error("{}: no processes containing {} found to send {} to.",
                id, javaProcessName, signalName);
        } else {
            log.info("{}: sending {} to {} pid(s) {}",
                id, signalName, javaProcessName, Utils.join(pids, ", "));
            for (Integer pid : pids) {
                platform.runCommand(new String[] {"kill", "-" + signalName, pid.toString()});
            }
        }
    }
}
