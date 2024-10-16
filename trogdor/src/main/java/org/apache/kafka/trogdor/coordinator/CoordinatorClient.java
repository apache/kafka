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

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.StringFormatter;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.DestroyTaskRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.JsonRestServer.HttpResponse;
import org.apache.kafka.trogdor.rest.RequestConflictException;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskPending;
import org.apache.kafka.trogdor.rest.TaskRequest;
import org.apache.kafka.trogdor.rest.TaskRunning;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TaskStateType;
import org.apache.kafka.trogdor.rest.TaskStopping;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.UptimeResponse;
import org.apache.kafka.trogdor.task.TaskSpec;

import com.fasterxml.jackson.core.type.TypeReference;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.UriBuilder;

import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;
import static org.apache.kafka.trogdor.common.StringFormatter.dateString;
import static org.apache.kafka.trogdor.common.StringFormatter.durationString;

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
            JsonRestServer.httpRequest(url("/coordinator/status"), "GET",
                null, new TypeReference<CoordinatorStatusResponse>() { }, maxTries);
        return resp.body();
    }

    public UptimeResponse uptime() throws Exception {
        HttpResponse<UptimeResponse> resp =
            JsonRestServer.httpRequest(url("/coordinator/uptime"), "GET",
                null, new TypeReference<UptimeResponse>() { }, maxTries);
        return resp.body();
    }

    public void createTask(CreateTaskRequest request) throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.httpRequest(log, url("/coordinator/task/create"), "POST",
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
        uriBuilder.queryParam("taskId", request.taskIds().toArray(new Object[0]));
        uriBuilder.queryParam("firstStartMs", request.firstStartMs());
        uriBuilder.queryParam("lastStartMs", request.lastStartMs());
        uriBuilder.queryParam("firstEndMs", request.firstEndMs());
        uriBuilder.queryParam("lastEndMs", request.lastEndMs());
        if (request.state().isPresent()) {
            uriBuilder.queryParam("state", request.state().get().toString());
        }
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

    private static void addTargetArgument(ArgumentParser parser) {
        parser.addArgument("--target", "-t")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("target")
            .metavar("TARGET")
            .help("A colon-separated host and port pair.  For example, example.com:8889");
    }

    private static void addJsonArgument(ArgumentParser parser) {
        parser.addArgument("--json")
            .action(storeTrue())
            .dest("json")
            .metavar("JSON")
            .help("Show the full response as JSON.");
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser rootParser = ArgumentParsers
            .newArgumentParser("trogdor-coordinator-client")
            .description("The Trogdor coordinator client.");
        Subparsers subParsers = rootParser.addSubparsers().
            dest("command");
        Subparser uptimeParser = subParsers.addParser("uptime")
            .help("Get the coordinator uptime.");
        addTargetArgument(uptimeParser);
        addJsonArgument(uptimeParser);
        Subparser statusParser = subParsers.addParser("status")
            .help("Get the coordinator status.");
        addTargetArgument(statusParser);
        addJsonArgument(statusParser);
        Subparser showTaskParser = subParsers.addParser("showTask")
            .help("Show a coordinator task.");
        addTargetArgument(showTaskParser);
        addJsonArgument(showTaskParser);
        showTaskParser.addArgument("--id", "-i")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskId")
            .metavar("TASK_ID")
            .help("The task ID to show.");
        showTaskParser.addArgument("--verbose", "-v")
            .action(storeTrue())
            .dest("verbose")
            .metavar("VERBOSE")
            .help("Print out everything.");
        showTaskParser.addArgument("--show-status", "-S")
            .action(storeTrue())
            .dest("showStatus")
            .metavar("SHOW_STATUS")
            .help("Show the task status.");
        Subparser showTasksParser = subParsers.addParser("showTasks")
            .help("Show many coordinator tasks.  By default, all tasks are shown, but " +
                "command-line options can be specified as filters.");
        addTargetArgument(showTasksParser);
        addJsonArgument(showTasksParser);
        MutuallyExclusiveGroup idGroup = showTasksParser.addMutuallyExclusiveGroup();
        idGroup.addArgument("--id", "-i")
            .action(append())
            .type(String.class)
            .dest("taskIds")
            .metavar("TASK_IDS")
            .help("Show only this task ID.  This option may be specified multiple times.");
        idGroup.addArgument("--id-pattern")
            .action(store())
            .type(String.class)
            .dest("taskIdPattern")
            .metavar("TASK_ID_PATTERN")
            .help("Only display tasks which match the given ID pattern.");
        showTasksParser.addArgument("--state", "-s")
            .type(TaskStateType.class)
            .dest("taskStateType")
            .metavar("TASK_STATE_TYPE")
            .help("Show only tasks in this state.");
        Subparser createTaskParser = subParsers.addParser("createTask")
            .help("Create a new task.");
        addTargetArgument(createTaskParser);
        createTaskParser.addArgument("--id", "-i")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskId")
            .metavar("TASK_ID")
            .help("The task ID to create.");
        createTaskParser.addArgument("--spec", "-s")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskSpec")
            .metavar("TASK_SPEC")
            .help("The task spec to create, or a path to a file containing the task spec.");
        Subparser stopTaskParser = subParsers.addParser("stopTask")
            .help("Stop a task.");
        addTargetArgument(stopTaskParser);
        stopTaskParser.addArgument("--id", "-i")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskId")
            .metavar("TASK_ID")
            .help("The task ID to create.");
        Subparser destroyTaskParser = subParsers.addParser("destroyTask")
            .help("Destroy a task.");
        addTargetArgument(destroyTaskParser);
        destroyTaskParser.addArgument("--id", "-i")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("taskId")
            .metavar("TASK_ID")
            .help("The task ID to destroy.");
        Subparser shutdownParser = subParsers.addParser("shutdown")
            .help("Shut down the coordinator.");
        addTargetArgument(shutdownParser);

        Namespace res = rootParser.parseArgsOrFail(args);
        String target = res.getString("target");
        CoordinatorClient client = new Builder().
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
                    System.out.printf("Coordinator is running at %s.%n", target);
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
                CoordinatorStatusResponse response = client.status();
                if (res.getBoolean("json")) {
                    System.out.println(JsonUtil.toJsonString(response));
                } else {
                    System.out.printf("Coordinator is running at %s.%n", target);
                    System.out.printf("\tStart time: %s%n", dateString(response.serverStartMs(), localOffset));
                }
                break;
            }
            case "showTask": {
                String taskId = res.getString("taskId");
                TaskRequest req = new TaskRequest(taskId);
                TaskState taskState = null;
                try {
                    taskState = client.task(req);
                } catch (NotFoundException e) {
                    System.out.printf("Task %s was not found.%n", taskId);
                    Exit.exit(1);
                }
                if (res.getBoolean("json")) {
                    System.out.println(JsonUtil.toJsonString(taskState));
                } else {
                    System.out.printf("Task %s of type %s is %s. %s%n", taskId,
                        taskState.spec().getClass().getCanonicalName(),
                        taskState.stateType(), prettyPrintTaskInfo(taskState, localOffset));
                    if (taskState instanceof TaskDone) {
                        TaskDone taskDone = (TaskDone) taskState;
                        if ((taskDone.error() != null) && (!taskDone.error().isEmpty())) {
                            System.out.printf("Error: %s%n", taskDone.error());
                        }
                    }
                    if (res.getBoolean("verbose")) {
                        System.out.printf("Spec: %s%n%n", JsonUtil.toPrettyJsonString(taskState.spec()));
                    }
                    if (res.getBoolean("verbose") || res.getBoolean("showStatus")) {
                        System.out.printf("Status: %s%n%n", JsonUtil.toPrettyJsonString(taskState.status()));
                    }
                }
                break;
            }
            case "showTasks": {
                TaskStateType taskStateType = res.get("taskStateType");
                List<String> taskIds = new ArrayList<>();
                Pattern taskIdPattern = null;
                if (res.getList("taskIds") != null) {
                    for (Object taskId : res.getList("taskIds")) {
                        taskIds.add((String) taskId);
                    }
                } else if (res.getString("taskIdPattern") != null) {
                    try {
                        taskIdPattern = Pattern.compile(res.getString("taskIdPattern"));
                    } catch (PatternSyntaxException e) {
                        System.out.println("Invalid task ID regular expression " + res.getString("taskIdPattern"));
                        e.printStackTrace();
                        Exit.exit(1);
                    }
                }
                TasksRequest req = new TasksRequest(taskIds, 0, 0, 0, 0,
                    Optional.ofNullable(taskStateType));
                TasksResponse response = client.tasks(req);
                if (taskIdPattern != null) {
                    TreeMap<String, TaskState> filteredTasks = new TreeMap<>();
                    for (Map.Entry<String, TaskState> entry : response.tasks().entrySet()) {
                        if (taskIdPattern.matcher(entry.getKey()).matches()) {
                            filteredTasks.put(entry.getKey(), entry.getValue());
                        }
                    }
                    response = new TasksResponse(filteredTasks);
                }
                if (res.getBoolean("json")) {
                    System.out.println(JsonUtil.toJsonString(response));
                } else {
                    System.out.println(prettyPrintTasksResponse(response, localOffset));
                }
                if (response.tasks().isEmpty()) {
                    Exit.exit(1);
                }
                break;
            }
            case "createTask": {
                String taskId = res.getString("taskId");
                TaskSpec taskSpec = JsonUtil.
                    objectFromCommandLineArgument(res.getString("taskSpec"), TaskSpec.class);
                CreateTaskRequest req = new CreateTaskRequest(taskId, taskSpec);
                try {
                    client.createTask(req);
                    System.out.printf("Sent CreateTaskRequest for task %s.%n", req.id());
                } catch (RequestConflictException rce) {
                    System.out.printf("CreateTaskRequest for task %s got a 409 status code - " +
                        "a task with the same ID but a different specification already exists.%nException: %s%n",
                        req.id(), rce.getMessage());
                    Exit.exit(1);
                }
                break;
            }
            case "stopTask": {
                String taskId = res.getString("taskId");
                StopTaskRequest req = new StopTaskRequest(taskId);
                client.stopTask(req);
                System.out.printf("Sent StopTaskRequest for task %s.%n", taskId);
                break;
            }
            case "destroyTask": {
                String taskId = res.getString("taskId");
                DestroyTaskRequest req = new DestroyTaskRequest(taskId);
                client.destroyTask(req);
                System.out.printf("Sent DestroyTaskRequest for task %s.%n", taskId);
                break;
            }
            case "shutdown": {
                client.shutdown();
                System.out.println("Sent ShutdownRequest.");
                break;
            }
            default: {
                System.out.println("You must choose an action. Type --help for help.");
                Exit.exit(1);
            }
        }
    }

    static String prettyPrintTasksResponse(TasksResponse response, ZoneOffset zoneOffset) {
        if (response.tasks().isEmpty()) {
            return "No matching tasks found.";
        }
        List<List<String>> lines = new ArrayList<>();
        List<String> header = new ArrayList<>(
            Arrays.asList("ID", "TYPE", "STATE", "INFO"));
        lines.add(header);
        for (Map.Entry<String, TaskState> entry : response.tasks().entrySet()) {
            String taskId = entry.getKey();
            TaskState taskState = entry.getValue();
            List<String> cols = new ArrayList<>();
            cols.add(taskId);
            cols.add(taskState.spec().getClass().getCanonicalName());
            cols.add(taskState.stateType().toString());
            cols.add(prettyPrintTaskInfo(taskState, zoneOffset));
            lines.add(cols);
        }
        return StringFormatter.prettyPrintGrid(lines);
    }

    static String prettyPrintTaskInfo(TaskState taskState, ZoneOffset zoneOffset) {
        if (taskState instanceof TaskPending) {
            return "Will start at " + dateString(taskState.spec().startMs(), zoneOffset);
        } else if (taskState instanceof TaskRunning) {
            TaskRunning runState = (TaskRunning) taskState;
            return "Started " + dateString(runState.startedMs(), zoneOffset) +
                "; will stop after " + durationString(taskState.spec().durationMs());
        } else if (taskState instanceof TaskStopping) {
            TaskStopping stoppingState = (TaskStopping) taskState;
            return "Started " + dateString(stoppingState.startedMs(), zoneOffset);
        } else if (taskState instanceof TaskDone) {
            TaskDone doneState = (TaskDone) taskState;
            String status;
            if (doneState.error() == null || doneState.error().isEmpty()) {
                if (doneState.cancelled()) {
                    status = "CANCELLED";
                } else {
                    status = "FINISHED";
                }
            } else {
                status = "FAILED";
            }
            return String.format("%s at %s after %s", status,
                dateString(doneState.doneMs(), zoneOffset),
                durationString(doneState.doneMs() - doneState.startedMs()));
        } else {
            throw new RuntimeException("Unknown task state type " + taskState.stateType());
        }
    }
}
