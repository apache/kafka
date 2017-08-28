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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.trogdor.agent.AgentClient;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.fault.FaultSpec;
import org.apache.kafka.trogdor.fault.FaultState;
import org.apache.kafka.trogdor.rest.AgentFaultsResponse;
import org.apache.kafka.trogdor.rest.CoordinatorFaultsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

public class ExpectedFaults {
    private static final Logger log = LoggerFactory.getLogger(ExpectedFaults.class);

    private static class FaultData {
        final FaultSpec spec;
        final FaultState state;

        FaultData(FaultSpec spec, FaultState state) {
            this.spec = spec;
            this.state = state;
        }
    }

    private interface FaultFetcher {
        TreeMap<String, FaultData> fetch() throws Exception;
    }

    private static class AgentFaultFetcher implements FaultFetcher {
        private final AgentClient client;

        AgentFaultFetcher(AgentClient client) {
            this.client = client;
        }

        @Override
        public TreeMap<String, FaultData> fetch() throws Exception {
            TreeMap<String, FaultData> results = new TreeMap<>();
            AgentFaultsResponse response = client.getFaults();
            for (Map.Entry<String, AgentFaultsResponse.FaultData> entry :
                    response.faults().entrySet()) {
                results.put(entry.getKey(),
                    new FaultData(entry.getValue().spec(), entry.getValue().state()));
            }
            return results;
        }
    }

    private static class CoordinatorFaultFetcher implements FaultFetcher {
        private final CoordinatorClient client;

        CoordinatorFaultFetcher(CoordinatorClient client) {
            this.client = client;
        }

        @Override
        public TreeMap<String, FaultData> fetch() throws Exception {
            TreeMap<String, FaultData> results = new TreeMap<>();
            CoordinatorFaultsResponse response = client.getFaults();
            for (Map.Entry<String, CoordinatorFaultsResponse.FaultData> entry :
                response.faults().entrySet()) {
                results.put(entry.getKey(),
                    new FaultData(entry.getValue().spec(), entry.getValue().state()));
            }
            return results;
        }
    }

    private final TreeMap<String, FaultData> expected = new TreeMap<String, FaultData>();

    public ExpectedFaults addFault(String id, FaultSpec spec) {
        expected.put(id, new FaultData(spec, null));
        return this;
    }

    public ExpectedFaults addFault(String id, FaultState state) {
        expected.put(id, new FaultData(null, state));
        return this;
    }

    public ExpectedFaults addFault(String id, FaultSpec spec, FaultState state) {
        expected.put(id, new FaultData(spec, state));
        return this;
    }

    public ExpectedFaults waitFor(AgentClient agentClient) throws InterruptedException {
        waitFor(new AgentFaultFetcher(agentClient));
        return this;
    }

    public ExpectedFaults waitFor(CoordinatorClient client) throws InterruptedException {
        waitFor(new CoordinatorFaultFetcher(client));
        return this;
    }

    private void waitFor(final FaultFetcher faultFetcher) throws InterruptedException {
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                TreeMap<String, FaultData> curData = null;
                try {
                    curData = faultFetcher.fetch();
                } catch (Exception e) {
                    log.info("Got error fetching faults", e);
                    throw new RuntimeException(e);
                }
                StringBuilder errors = new StringBuilder();
                for (Map.Entry<String, FaultData> entry : expected.entrySet()) {
                    String id = entry.getKey();
                    FaultData expectedFaultData = entry.getValue();
                    FaultData curFaultData = curData.get(id);
                    if (curFaultData == null) {
                        errors.append("Did not find fault id " + id + "\n");
                    } else {
                        if (expectedFaultData.spec != null) {
                            if (!expectedFaultData.spec.equals(curFaultData.spec)) {
                                errors.append("For fault id " + id + ", expected fault " +
                                    "spec " + expectedFaultData.spec + ", but got " +
                                    curFaultData.spec + "\n");
                            }
                        }
                        if (expectedFaultData.state != null) {
                            if (!expectedFaultData.state.equals(curFaultData.state)) {
                                errors.append("For fault id " + id + ", expected fault " +
                                    "state " + expectedFaultData.state + ", but got " +
                                    curFaultData.state + "\n");
                            }
                        }
                    }
                }
                for (String id : curData.keySet()) {
                    if (expected.get(id) == null) {
                        errors.append("Got unexpected fault id " + id + "\n");
                    }
                }
                String errorString = errors.toString();
                if (!errorString.isEmpty()) {
                    log.info("EXPECTED FAULTS: {}", faultsToString(expected));
                    log.info("ACTUAL FAULTS  : {}", faultsToString(curData));
                    log.info(errorString);
                    return false;
                }
                return true;
            }
        }, "Timed out waiting for expected fault specs " + faultsToString(expected));
    }

    private static String faultsToString(TreeMap<String, FaultData> faults) {
        StringBuilder bld = new StringBuilder();
        bld.append("{");
        String faultsPrefix = "";
        for (Map.Entry<String, FaultData> entry : faults.entrySet()) {
            String id = entry.getKey();
            bld.append(faultsPrefix).append(id).append(": {");
            faultsPrefix = ", ";
            String faultValuesPrefix = "";
            FaultData faultData = entry.getValue();
            if (faultData.spec != null) {
                bld.append(faultValuesPrefix).append("spec: ").append(faultData.spec);
                faultValuesPrefix = ", ";
            }
            if (faultData.state != null) {
                bld.append(faultValuesPrefix).append("state: ").append(faultData.state);
                faultValuesPrefix = ", ";
            }
            bld.append("}");
        }
        bld.append("}");
        return bld.toString();
    }
};
