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

package org.apache.kafka.castle.cloud;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.cluster.CastleNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockCloud implements Cloud {
    private final Logger log = LoggerFactory.getLogger(MockCloud.class);

    public static final class Settings {
        @Override
        public String toString() {
            return "MockCloud()";
        }
    }

    private final class CommandResponseTracker {
        private final String nodeName;
        private final HashMap<List<String>, ArrayDeque<MockCommandResponse>> commandResponses;
        private int commandResponsesRemoved;

        CommandResponseTracker(String nodeName) {
            this.nodeName = nodeName;
            this.commandResponses = new HashMap<>();
            this.commandResponsesRemoved = 0;
        }

        public void add(MockCommandResponse response) {
            ArrayDeque<MockCommandResponse> queue = this.commandResponses.get(response.request());
            if (queue == null) {
                queue = new ArrayDeque<>();
                this.commandResponses.put(response.request(), queue);
            }
            log.trace("{}: CommandResponseTracker registered command line {}", nodeName,
                CastleRemoteCommand.joinCommandLineArgs(response.request()));
            queue.addLast(response);
        }

        public MockCommandResponse remove(List<String> commandLine) {
            ArrayDeque<MockCommandResponse> queue = this.commandResponses.get(commandLine);
            if (queue == null) {
                return null;
            }
            MockCommandResponse response = queue.removeFirst();
            if (queue.isEmpty()) {
                this.commandResponses.remove(commandLine);
            }
            if (response != null) {
                commandResponsesRemoved++;
            }
            return response;
        }

        public int commandResponsesRemoved() {
            return commandResponsesRemoved;
        }
    }

    private final Settings settings;

    private Map<String, InstanceDescription> instances = new HashMap<>();

    private final HashMap<String, CommandResponseTracker> commandResponseTrackers = new HashMap<>();

    private int idCounter = 0;

    class MockRunner extends Cloud.Runner {
        @Override
        public String run() throws Exception {
            synchronized (MockCloud.this) {
                String instanceId = nextId();
                InstanceDescription instance = new InstanceDescription(instanceId,
                    privateDns(instanceId),
                    publicDns(instanceId),
                    "running");
                instances.put(instanceId, instance);
                log.trace("runInstance: created instance {}", instance);
                return instanceId;
            }
        }
    }

    public MockCloud(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Runner newRunner() {
        return new MockRunner();
    }

    private synchronized String nextId() {
        return String.format("inst%02d", idCounter++);
    }

    @Override
    public synchronized InstanceDescription describeInstance(String instanceId) throws Exception {
        return instances.get(instanceId);
    }

    @Override
    public synchronized Collection<InstanceDescription> describeInstances() throws Exception {
        ArrayList<InstanceDescription> descriptions = new ArrayList<>();
        descriptions.addAll(instances.values());
        return descriptions;
    }

    public static String publicDns(String instanceId) {
        return String.format("%s.public.example.com", instanceId);
    }

    public static String privateDns(String instanceId) {
        return String.format("%s.private.example.com", instanceId);
    }

    @Override
    public synchronized void terminateInstances(String... instanceIds) throws Exception {
        RuntimeException exception = null;
        List<String> terminated = new ArrayList<>();
        for (String instanceId : instanceIds) {
            if (instances.remove(instanceId) == null) {
                if (exception == null) {
                    exception = new RuntimeException("instance " + instanceId + " is not running.");
                }
            } else {
                terminated.add(instanceId);
            }
        }
        log.trace("terminated instance(s) {}", Utils.join(terminated, ", "));
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public RemoteCommand remoteCommand(CastleNode node) {
        return new MockRemoteCommand(this, node);
    }

    public synchronized void addCommandResponse(String nodeName, MockCommandResponse response) {
        CommandResponseTracker tracker = commandResponseTrackers.get(nodeName);
        if (tracker == null) {
            tracker = new CommandResponseTracker(nodeName);
            commandResponseTrackers.put(nodeName, tracker);
        }
        tracker.add(response);
    }

    public synchronized MockCommandResponse removeResponse(String nodeName, List<String> commandLine) {
        CommandResponseTracker tracker = commandResponseTrackers.get(nodeName);
        if (tracker == null) {
            log.trace("removeResponse({}, {}): no tracker found for {}",
                nodeName, CastleRemoteCommand.joinCommandLineArgs(commandLine), nodeName);
            return null;
        }
        MockCommandResponse response = tracker.remove(commandLine);
        return response;
    }

    public synchronized int commandResponsesRemoved(String nodeName) {
        CommandResponseTracker tracker = commandResponseTrackers.get(nodeName);
        if (tracker == null) {
            return 0;
        }
        return tracker.commandResponsesRemoved();
    }

    public synchronized int numInstances() {
        return instances.size();
    }

    @Override
    public void close() throws Exception {
        // Nothing to do here
    }

    @Override
    public String toString() {
        return settings.toString();
    }
}
