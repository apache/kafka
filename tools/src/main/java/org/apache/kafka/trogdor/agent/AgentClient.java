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

import com.fasterxml.jackson.core.type.TypeReference;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.JsonRestServer.HttpResponse;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * A client for the Trogdor agent.
 */
public class AgentClient {
    private final Logger log;

    /**
     * The maximum number of tries to make.
     */
    private final int maxTries;

    /**
     * The URL target.
     */
    private final String target;

    public static class Builder {
        private Logger log = LoggerFactory.getLogger(AgentClient.class);
        private int maxTries = 1;
        private String target = null;

        public Builder() {
        }

        public Builder log(Logger log) {
            this.log = log;
            return this;
        }

        public Builder maxTries(int maxTries) {
            this.maxTries = maxTries;
            return this;
        }

        public Builder target(String target) {
            this.target = target;
            return this;
        }

        public Builder target(String host, int port) {
            this.target = String.format("%s:%d", host, port);
            return this;
        }

        public AgentClient build() {
            if (target == null) {
                throw new RuntimeException("You must specify a target.");
            }
            return new AgentClient(log, maxTries, target);
        }
    }

    private AgentClient(Logger log, int maxTries, String target) {
        this.log = log;
        this.maxTries = maxTries;
        this.target = target;
    }

    public String target() {
        return target;
    }

    public int maxTries() {
        return maxTries;
    }

    private String url(String suffix) {
        return String.format("http://%s%s", target, suffix);
    }

    public AgentStatusResponse status() throws Exception {
        HttpResponse<AgentStatusResponse> resp =
            JsonRestServer.<AgentStatusResponse>httpRequest(url("/agent/status"), "GET",
                null, new TypeReference<AgentStatusResponse>() { }, maxTries);
        return resp.body();
    }

    public void createWorker(CreateWorkerRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.<Empty>httpRequest(
                url("/agent/worker/create"), "POST",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void stopWorker(StopWorkerRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.<Empty>httpRequest(url(
                "/agent/worker/stop"), "PUT",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void destroyWorker(DestroyWorkerRequest request) throws Exception {
        UriBuilder uriBuilder = UriBuilder.fromPath(url("/agent/worker"));
        uriBuilder.queryParam("workerId", request.workerId());
        HttpResponse<Empty> resp =
            JsonRestServer.<Empty>httpRequest(uriBuilder.build().toString(), "DELETE",
                null, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void invokeShutdown() throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.<Empty>httpRequest(url(
                "/agent/shutdown"), "PUT",
                null, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-agent-client")
            .defaultHelp(true)
            .description("The Trogdor fault injection agent client.");
        parser.addArgument("target")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("target")
            .metavar("TARGET")
            .help("A colon-separated host and port pair.  For example, example.com:8888");
        MutuallyExclusiveGroup actions = parser.addMutuallyExclusiveGroup();
        actions.addArgument("--status")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("status")
            .help("Get agent status.");
        actions.addArgument("--create-worker")
            .action(store())
            .type(String.class)
            .dest("create_worker")
            .metavar("SPEC_JSON")
            .help("Create a new fault.");
        actions.addArgument("--stop-worker")
            .action(store())
            .type(Long.class)
            .dest("stop_worker")
            .metavar("WORKER_ID")
            .help("Stop a worker ID.");
        actions.addArgument("--destroy-worker")
            .action(store())
            .type(Long.class)
            .dest("destroy_worker")
            .metavar("WORKER_ID")
            .help("Destroy a worker ID.");
        actions.addArgument("--shutdown")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("shutdown")
            .help("Trigger agent shutdown");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }
        String target = res.getString("target");
        AgentClient client = new Builder().
            maxTries(3).
            target(target).
            build();
        if (res.getBoolean("status")) {
            System.out.println("Got agent status: " +
                JsonUtil.toPrettyJsonString(client.status()));
        } else if (res.getString("create_worker") != null) {
            CreateWorkerRequest req = JsonUtil.JSON_SERDE.
                readValue(res.getString("create_worker"), CreateWorkerRequest.class);
            client.createWorker(req);
            System.out.printf("Sent CreateWorkerRequest for worker %d%n.", req.workerId());
        } else if (res.getString("stop_worker") != null) {
            long workerId = res.getLong("stop_worker");
            client.stopWorker(new StopWorkerRequest(workerId));
            System.out.printf("Sent StopWorkerRequest for worker %d%n.", workerId);
        } else if (res.getString("destroy_worker") != null) {
            long workerId = res.getLong("stop_worker");
            client.destroyWorker(new DestroyWorkerRequest(workerId));
            System.out.printf("Sent DestroyWorkerRequest for worker %d%n.", workerId);
        } else if (res.getBoolean("shutdown")) {
            client.invokeShutdown();
            System.out.println("Sent ShutdownRequest.");
        } else {
            System.out.println("You must choose an action. Type --help for help.");
            Exit.exit(1);
        }
    }
};
