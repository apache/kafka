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

package org.apache.kafka.trogdor.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateTasksRequest;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.DestroyTaskRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.JsonRestServer.HttpResponse;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.TaskRequest;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriBuilder;

import java.util.Optional;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * A client for the Trogdor coordinator.
 */
public class CoordinatorClient {
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
        private Logger log = LoggerFactory.getLogger(CoordinatorClient.class);
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

        public CoordinatorClient build() {
            if (target == null) {
                throw new RuntimeException("You must specify a target.");
            }
            return new CoordinatorClient(log, maxTries, target);
        }
    }

    private CoordinatorClient(Logger log, int maxTries, String target) {
        this.log = log;
        this.maxTries = maxTries;
        this.target = target;
    }

    public int maxTries() {
        return maxTries;
    }

    private String url(String suffix) {
        return String.format("http://%s%s", target, suffix);
    }

    public CoordinatorStatusResponse status() throws Exception {
        HttpResponse<CoordinatorStatusResponse> resp =
            JsonRestServer.<CoordinatorStatusResponse>httpRequest(url("/coordinator/status"), "GET",
                null, new TypeReference<CoordinatorStatusResponse>() { }, maxTries);
        return resp.body();
    }

    public void createTask(CreateTaskRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(log, url("/coordinator/task/create"), "POST",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void createTasks(CreateTasksRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(log, url("/coordinator/task/creates"), "POST",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void stopTask(StopTaskRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(log, url("/coordinator/task/stop"), "PUT",
                request, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public void destroyTask(DestroyTaskRequest request) throws Exception {
        UriBuilder uriBuilder = UriBuilder.fromPath(url("/coordinator/tasks"));
        uriBuilder.queryParam("taskId", request.id());
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(log, uriBuilder.build().toString(), "DELETE",
                null, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public TasksResponse tasks(TasksRequest request) throws Exception {
        UriBuilder uriBuilder = UriBuilder.fromPath(url("/coordinator/tasks"));
        uriBuilder.queryParam("taskId", (Object[]) request.taskIds().toArray(new String[0]));
        uriBuilder.queryParam("firstStartMs", request.firstStartMs());
        uriBuilder.queryParam("lastStartMs", request.lastStartMs());
        uriBuilder.queryParam("firstEndMs", request.firstEndMs());
        uriBuilder.queryParam("lastEndMs", request.lastEndMs());
        HttpResponse<TasksResponse> resp =
            JsonRestServer.httpRequest(log, uriBuilder.build().toString(), "GET",
                null, new TypeReference<TasksResponse>() { }, maxTries);
        return resp.body();
    }

    public TaskState task(TaskRequest request) throws Exception {
        String uri = UriBuilder.fromPath(url("/coordinator/tasks/{taskId}")).build(request.taskId()).toString();
        HttpResponse<TaskState> resp = JsonRestServer.httpRequest(log, uri, "GET",
            null, new TypeReference<TaskState>() { }, maxTries);
        return resp.body();
    }

    public void shutdown() throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(log, url("/coordinator/shutdown"), "PUT",
                null, new TypeReference<Empty>() { }, maxTries);
        resp.body();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-coordinator-client")
            .defaultHelp(true)
            .description("The Trogdor fault injection coordinator client.");
        parser.addArgument("target")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("target")
            .metavar("TARGET")
            .help("A colon-separated host and port pair.  For example, example.com:8889");
        MutuallyExclusiveGroup actions = parser.addMutuallyExclusiveGroup();
        actions.addArgument("--status")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("status")
            .help("Get coordinator status.");
        actions.addArgument("--show-tasks")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("show_tasks")
            .help("Show coordinator tasks.");
        actions.addArgument("--show-task")
            .action(store())
            .type(String.class)
            .dest("show_task")
            .metavar("TASK_ID")
            .help("Show a specific coordinator task.");
        actions.addArgument("--create-task")
            .action(store())
            .type(String.class)
            .dest("create_task")
            .metavar("TASK_SPEC_JSON")
            .help("Create a new task from a task spec.");
        actions.addArgument("--create-tasks")
            .action(store())
            .type(String.class)
            .dest("create_tasks")
            .metavar("TASK_SPECS_JSON")
            .help("Create new tasks from multiple task specs.");
        actions.addArgument("--stop-task")
            .action(store())
            .type(String.class)
            .dest("stop_task")
            .metavar("TASK_ID")
            .help("Stop a task.");
        actions.addArgument("--destroy-task")
            .action(store())
            .type(String.class)
            .dest("destroy_task")
            .metavar("TASK_ID")
            .help("Destroy a task.");
        actions.addArgument("--shutdown")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("shutdown")
            .help("Trigger coordinator shutdown");

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
        CoordinatorClient client = new Builder().
            maxTries(3).
            target(target).
            build();
        if (res.getBoolean("status")) {
            System.out.println("Got coordinator status: " +
                JsonUtil.toPrettyJsonString(client.status()));
        } else if (res.getBoolean("show_tasks")) {
            System.out.println("Got coordinator tasks: " +
                JsonUtil.toPrettyJsonString(client.tasks(
                    new TasksRequest(null, 0, 0, 0, 0, Optional.empty()))));
        } else if (res.getString("show_task") != null) {
            String taskId = res.getString("show_task");
            TaskRequest req = new TaskRequest(res.getString("show_task"));
            try {
                String taskOutput = String.format("Got coordinator task \"%s\": %s", taskId, JsonUtil.toPrettyJsonString(client.task(req)));
                System.out.println(taskOutput);
            } catch (NotFoundException e) {
                System.out.println(e.getMessage());
            }
        } else if (res.getString("create_task") != null) {
            CreateTaskRequest req = JsonUtil.JSON_SERDE.
                readValue(res.getString("create_task"), CreateTaskRequest.class);
            client.createTask(req);
            System.out.printf("Sent CreateTaskRequest for task %s.", req.id());
        } else if (res.getString("create_tasks") != null) {
            CreateTasksRequest req = JsonUtil.JSON_SERDE.
                readValue(res.getString("create_tasks"), CreateTasksRequest.class);
            client.createTasks(req);
            System.out.print("Sent CreateTasksRequest for multiple tasks.");
        } else if (res.getString("stop_task") != null) {
            String taskId = res.getString("stop_task");
            client.stopTask(new StopTaskRequest(taskId));
            System.out.printf("Sent StopTaskRequest for task %s.%n", taskId);
        } else if (res.getString("destroy_task") != null) {
            String taskId = res.getString("destroy_task");
            client.destroyTask(new DestroyTaskRequest(taskId));
            System.out.printf("Sent DestroyTaskRequest for task %s.%n", taskId);
        } else if (res.getBoolean("shutdown")) {
            client.shutdown();
            System.out.println("Sent ShutdownRequest.");
        } else {
            System.out.println("You must choose an action. Type --help for help.");
            Exit.exit(1);
        }
    }
};
