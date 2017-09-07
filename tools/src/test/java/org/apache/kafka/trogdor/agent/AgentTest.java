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

package org.apache.kafka.trogdor.agent;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.basic.BasicNode;
import org.apache.kafka.trogdor.basic.BasicPlatform;
import org.apache.kafka.trogdor.basic.BasicTopology;
import org.apache.kafka.trogdor.common.ExpectedFaults;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.fault.DoneState;
import org.apache.kafka.trogdor.fault.NoOpFaultSpec;
import org.apache.kafka.trogdor.fault.RunningState;
import org.apache.kafka.trogdor.rest.AgentFaultsResponse;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateAgentFaultRequest;

import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class AgentTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private static BasicPlatform createBasicPlatform() {
        TreeMap<String, Node> nodes = new TreeMap<>();
        HashMap<String, String> config = new HashMap<>();
        nodes.put("node01", new BasicNode("node01", "localhost",
            config, Collections.<String>emptySet()));
        BasicTopology topology = new BasicTopology(nodes);
        return new BasicPlatform("node01", topology, new BasicPlatform.ShellCommandRunner());
    }

    private Agent createAgent(Time time) {
        JsonRestServer restServer = new JsonRestServer(0);
        AgentRestResource resource = new AgentRestResource();
        restServer.start(resource);
        return new Agent(createBasicPlatform(), time, restServer, resource);
    }

    @Test
    public void testAgentStartShutdown() throws Exception {
        Agent agent = createAgent(Time.SYSTEM);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentProgrammaticShutdown() throws Exception {
        Agent agent = createAgent(Time.SYSTEM);
        AgentClient client = new AgentClient("localhost", agent.port());
        client.invokeShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentGetStatus() throws Exception {
        Agent agent = createAgent(Time.SYSTEM);
        AgentClient client = new AgentClient("localhost", agent.port());
        AgentStatusResponse status = client.getStatus();
        assertEquals(agent.startTimeMs(), status.startTimeMs());
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentCreateFaults() throws Exception {
        Time time = new MockTime(0, 0, 0);
        Agent agent = createAgent(time);
        AgentClient client = new AgentClient("localhost", agent.port());
        AgentFaultsResponse faults = client.getFaults();
        assertEquals(Collections.emptyMap(), faults.faults());
        new ExpectedFaults().waitFor(client);

        final NoOpFaultSpec fooSpec = new NoOpFaultSpec(1000, 600000);
        client.putFault(new CreateAgentFaultRequest("foo", fooSpec));
        new ExpectedFaults().addFault("foo", fooSpec).waitFor(client);

        final NoOpFaultSpec barSpec = new NoOpFaultSpec(2000, 900000);
        client.putFault(new CreateAgentFaultRequest("bar", barSpec));
        new ExpectedFaults().
            addFault("foo", fooSpec).
            addFault("bar", barSpec).
            waitFor(client);

        final NoOpFaultSpec bazSpec = new NoOpFaultSpec(1, 450000);
        client.putFault(new CreateAgentFaultRequest("baz", bazSpec));
        new ExpectedFaults().
            addFault("foo", fooSpec).
            addFault("bar", barSpec).
            addFault("baz", bazSpec).
            waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentActivatesFaults() throws Exception {
        Time time = new MockTime(0, 0, 0);
        Agent agent = createAgent(time);
        AgentClient client = new AgentClient("localhost", agent.port());
        AgentFaultsResponse faults = client.getFaults();
        assertEquals(Collections.emptyMap(), faults.faults());
        new ExpectedFaults().waitFor(client);

        final NoOpFaultSpec fooSpec = new NoOpFaultSpec(10, 2);
        client.putFault(new CreateAgentFaultRequest("foo", fooSpec));
        new ExpectedFaults().addFault("foo", new RunningState(0)).waitFor(client);

        time.sleep(3);
        new ExpectedFaults().addFault("foo", new DoneState(3, "")).waitFor(client);

        final NoOpFaultSpec barSpec = new NoOpFaultSpec(20, 3);
        client.putFault(new CreateAgentFaultRequest("bar", barSpec));
        new ExpectedFaults().
            addFault("foo", new DoneState(3, "")).
            addFault("bar", new RunningState(3)).
            waitFor(client);

        time.sleep(4);
        final NoOpFaultSpec bazSpec = new NoOpFaultSpec(1, 2);
        client.putFault(new CreateAgentFaultRequest("baz", bazSpec));
        new ExpectedFaults().
            addFault("foo", new DoneState(3, "")).
            addFault("bar", new DoneState(7, "")).
            addFault("baz", new RunningState(7)).
            waitFor(client);

        time.sleep(3);
        new ExpectedFaults().
            addFault("foo", new DoneState(3, "")).
            addFault("bar", new DoneState(7, "")).
            addFault("baz", new DoneState(10, "")).
            waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }
};
