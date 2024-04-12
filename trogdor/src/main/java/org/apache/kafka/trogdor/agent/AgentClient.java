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
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.StringFormatter;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.JsonRestServer.HttpResponse;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.apache.kafka.trogdor.rest.WorkerState;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.rest.UptimeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;
import static org.apache.kafka.trogdor.common.StringFormatter.dateString;
import static org.apache.kafka.trogdor.common.StringFormatter.durationString;

/**
 * A client for the Trogdor agent.
 */
public class AgentClient {

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
            JsonRestServer.httpRequest(url("/agent/status"), "GET",
                null, new TypeReference<AgentStatusResponse>() { }, maxTries);
        return resp.body();
    }

    public UptimeResponse uptime() throws Exception {
        HttpResponse<UptimeResponse> resp =
            JsonRestServer.httpRequest(url("/agent/uptime"), "GET",
                null, new TypeReference<UptimeResponse>() { }, maxTries);
        return resp.body();
    }

    public void createWorker(CreateWorkerRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(
                url("/agent/worker/create"), "POST",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void stopWorker(StopWorkerRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(url(
                "/agent/worker/stop"), "PUT",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void destroyWorker(DestroyWorkerRequest request) throws Exception {
        UriBuilder uriBuilder = UriBuilder.fromPath(url("/agent/worker"));
        uriBuilder.queryParam("workerId", request.workerId());
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(uriBuilder.build().toString(), "DELETE",
                null, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void invokeShutdown() throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(url(
                "/agent/shutdown"), "PUT",
                null, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    private static void addTargetArgument(ArgumentParser parser) {
        parser.addArgument("--target", "-t")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("target")
            .metavar("TARGET")
            .help("A colon-separated host and port pair.  For example, example.com:8888");
    }

    private static void addJsonArgument(ArgumentParser parser) {
        parser.addArgument("--json")
            .action(storeTrue())
            .dest("json")
            .metavar("JSON")
            .help("Show the full response as JSON.");
    }

    private static void addWorkerIdArgument(ArgumentParser parser, String help) {
        parser.addArgument("--workerId")
            .action(storeTrue())
            .type(Long.class)
            .dest("workerId")
            .metavar("WORKER_ID")
            .help(help);
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser rootParser = ArgumentParsers
            .newArgumentParser("trogdor-agent-client")
            .defaultHelp(true)
            .description("The Trogdor agent client.");
        Subparsers subParsers = rootParser.addSubparsers().
            dest("command");
        Subparser uptimeParser = subParsers.addParser("uptime")
            .help("Get the agent uptime.");
        addTargetArgument(uptimeParser);
        addJsonArgument(uptimeParser);
        Subparser statusParser = subParsers.addParser("status")
            .help("Get the agent status.");
        addTargetArgument(statusParser);
        addJsonArgument(statusParser);
        Subparser createWorkerParser = subParsers.addParser("createWorker")
            .help("Create a new worker.");
        addTargetArgument(createWorkerParser);
        addWorkerIdArgument(createWorkerParser, "The worker ID to create.");
        createWorkerParser.addArgument("--taskId")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskId")
            .metavar("TASK_ID")
            .help("The task ID to create.");
        createWorkerParser.addArgument("--spec", "-s")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskSpec")
            .metavar("TASK_SPEC")
            .help("The task spec to create, or a path to a file containing the task spec.");
        Subparser stopWorkerParser = subParsers.addParser("stopWorker")
            .help("Stop a worker.");
        addTargetArgument(stopWorkerParser);
        addWorkerIdArgument(stopWorkerParser, "The worker ID to stop.");
        Subparser destroyWorkerParser = subParsers.addParser("destroyWorker")
            .help("Destroy a worker.");
        addTargetArgument(destroyWorkerParser);
        addWorkerIdArgument(destroyWorkerParser, "The worker ID to destroy.");
        Subparser shutdownParser = subParsers.addParser("shutdown")
            .help("Shut down the agent.");
        addTargetArgument(shutdownParser);

        Namespace res = rootParser.parseArgsOrFail(args);
        String target = res.getString("target");
        AgentClient client = new Builder().
            maxTries(3).
            target(target).
            build();
        ZoneOffset localOffset = OffsetDateTime.now().getOffset();
        switch (res.getString("command")) {
            case "uptime": {
                UptimeResponse uptime = client.uptime();
                if (res.getBoolean("json")) {
                    System.out.println(JsonUtil.toJsonString(uptime));
                } else {
                    System.out.printf("Agent is running at %s.%n", target);
                    System.out.printf("\tStart time: %s%n",
                        dateString(uptime.serverStartMs(), localOffset));
                    System.out.printf("\tCurrent server time: %s%n",
                        dateString(uptime.nowMs(), localOffset));
                    System.out.printf("\tUptime: %s%n",
                        durationString(uptime.nowMs() - uptime.serverStartMs()));
                }
                break;
            }
            case "status": {
                AgentStatusResponse status = client.status();
                if (res.getBoolean("json")) {
                    System.out.println(JsonUtil.toJsonString(status));
                } else {
                    System.out.printf("Agent is running at %s.%n", target);
                    System.out.printf("\tStart time: %s%n",
                        dateString(status.serverStartMs(), localOffset));
                    List<List<String>> lines = new ArrayList<>();
                    List<String> header = new ArrayList<>(
                        Arrays.asList("WORKER_ID", "TASK_ID", "STATE", "TASK_TYPE"));
                    lines.add(header);
                    for (Map.Entry<Long, WorkerState> entry : status.workers().entrySet()) {
                        List<String> cols = new ArrayList<>();
                        cols.add(Long.toString(entry.getKey()));
                        cols.add(entry.getValue().taskId());
                        cols.add(entry.getValue().getClass().getSimpleName());
                        cols.add(entry.getValue().spec().getClass().getCanonicalName());
                        lines.add(cols);
                    }
                    System.out.print(StringFormatter.prettyPrintGrid(lines));
                }
                break;
            }
            case "createWorker": {
                long workerId = res.getLong("workerId");
                String taskId = res.getString("taskId");
                TaskSpec taskSpec = JsonUtil.
                    objectFromCommandLineArgument(res.getString("taskSpec"), TaskSpec.class);
                CreateWorkerRequest req =
                    new CreateWorkerRequest(workerId, taskId, taskSpec);
                client.createWorker(req);
                System.out.printf("Sent CreateWorkerRequest for worker %d%n.", req.workerId());
                break;
            }
            case "stopWorker": {
                long workerId = res.getLong("workerId");
                client.stopWorker(new StopWorkerRequest(workerId));
                System.out.printf("Sent StopWorkerRequest for worker %d%n.", workerId);
                break;
            }
            case "destroyWorker": {
                long workerId = res.getLong("workerId");
                client.destroyWorker(new DestroyWorkerRequest(workerId));
                System.out.printf("Sent DestroyWorkerRequest for worker %d%n.", workerId);
                break;
            }
            case "shutdown": {
                client.invokeShutdown();
                System.out.println("Sent ShutdownRequest.");
                break;
            }
            default: {
                System.out.println("You must choose an action. Type --help for help.");
                Exit.exit(1);
            }
        }
    }
}
