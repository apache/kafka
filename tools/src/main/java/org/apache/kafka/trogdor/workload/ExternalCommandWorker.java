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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ExternalCommandWorker runs the Trogdor task with an external command.
 *
 * ExternalCommandWorker starts an external process to execute the command. Communication between the external process
 * and ExternalCommandWorker should follow these APIs:
 *
 * Control API: {"action":"stop"|"start", "spec":ExternalCommandSpec}
 *
 * ExternalCommandWorker can control the external process by sending commands to the process's stdin.
 *
 * Start a task {"action":"start", "spec":ExternalCommandSpec}.
 *
 * Stop a task {"action":"stop", "spec":ExternalCommandSpec}. The external process should exit after seeing a stop
 * command even there is no proceeding start command.
 *
 *
 *
 * Communication API: {"status":val, "log":val, "error":"msg"}
 *
 * The external process can sends JSON object messages to the ExternalCommandWorker via the process's stduot, and error
 * messages via the process's stderr.
 * ExternalCommandWorker should continuously monitor the stdout and the stderr of the external process line by line.
 *
 * For any JSON object the external process writes to its stdout, if the object has the "status" field, this worker should set
 * its status with the "status" value, if the object has the "error" field and the value is a JSON string, this worker should
 * set the agent error message with the "error" value, and if the object has the "log" field, this worker should put the "log"
 * value it its log.
 *
 * For any line the external process writes to its stderr, this worker should save the line to its error log and set the
 * error message with the line.
 */
public class ExternalCommandWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ExternalCommandWorker.class);

    private final String id;

    private final ExternalCommandSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService executor;

    private ExternalProcess ep;

    private String errMsg = "";

    private WorkerStatusTracker status;

    private KafkaFutureImpl<String> doneFuture;

    public ExternalCommandWorker(String id, ExternalCommandSpec spec) {
        this.id = id;
        this.spec = spec;

    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ExternalCommandWorker is already running.");
        }
        log.info("{}: Activating ExternalCommandWorker with {}", id, spec);
        this.executor = Executors.newCachedThreadPool(
                ThreadUtils.createThreadFactory("ExternalCommandWorkerThread%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        executor.submit(new ProcessMonitor());
    }

    public class ProcessMonitor implements Runnable {
        @Override
        public void run() {
            if (spec.command().isEmpty()) {
                errMsg = "No command specified";
                doneFuture.complete(errMsg);
                return;
            }

            try {
                ep = new ExternalProcess();
            } catch (IOException e) {
                errMsg = "Failed to start the external process.";
                log.error("{}: Failed to start the external process.");
                doneFuture.complete(errMsg);
                return;
            }

            try {
                ep.startTask();
                int workerExitValue = ep.waitForProcess();
                log.info("{}: The process finished with the exit value: {}.", id, workerExitValue);
                if (workerExitValue != 0 && errMsg.isEmpty()) {
                    errMsg = "ExternalWorker exited with error code " + workerExitValue;
                }
                log.info("{}: StatusUpdater and errorUpdater are terminated.", id);
            } catch (InterruptedException e) {
                log.info("{}: ProcessMonitor is interrupted.", id);
            } catch (ExecutionException e) {
                log.error("{}: StatusUpdaters throw an exception " + e);
            } finally {
                doneFuture.complete(errMsg);
            }
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ExternalCommandWorker is not running.");
        }
        log.info("{}: Deactivating ExternalCommandWorker.", id);
        if (ep != null) {
            ep.destroyProcess();
        }
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        this.executor = null;
        this.ep = null;
        this.status = null;
        this.doneFuture = null;
        this.ep = null;
    }

    public class ControlCommand {
        private final String action;
        private final ExternalCommandSpec spec;
        @JsonCreator
        ControlCommand(@JsonProperty("action") String action,
                       @JsonProperty("spec") ExternalCommandSpec spec) {
            this.action = action;
            this.spec = spec;
        }
        @JsonProperty
        public String action() {
            return action;
        }
        @JsonProperty
        public ExternalCommandSpec spec() {
            return spec;
        }

        public void issue(PrintWriter controlChannel) {
            String command = JsonUtil.toJsonString(this);
            log.info("{}: Send out command: {} ", id, command);
            controlChannel.println(command);
        }
    }

    public class ExternalProcess {
        private final Process process;
        private final PrintWriter controlChannel;
        Future stdoutMonitor;
        Future stderrMonitor;
        ExternalProcess() throws IOException {
            ProcessBuilder processBuilder = new ProcessBuilder(spec.command());
            this.process = processBuilder.start();
            this.controlChannel = new PrintWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8), true);
            this.stdoutMonitor = executor.submit(new StatusUpdater(this.process));
            this.stderrMonitor = executor.submit(new ErrorUpdater(this.process));
        }

        void startTask() {
            synchronized (this) {
                new ControlCommand("start", spec).issue(controlChannel);
            }
        }

        int waitForProcess() throws InterruptedException, ExecutionException {
            process.waitFor();
            stdoutMonitor.get();
            stderrMonitor.get();
            return process.exitValue();
        }
        void stopTask() {
            synchronized (this) {
                new ControlCommand("stop", spec).issue(controlChannel);
            }
        }
        void destroyProcess() throws InterruptedException {
            if (process.isAlive()) {
                this.stopTask();
                process.waitFor(1, TimeUnit.MINUTES);
                if (process.isAlive()) {
                    process.destroy();
                    process.waitFor();
                }
            }
        }
    }

    public class ErrorUpdater implements Runnable {
        private Process process;
        ErrorUpdater(Process process) {
            this.process = process;
        }
        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    log.error("{}: (stderr of the process):{}", id, line);
                }
            } catch (IOException ioe) {
                log.info("{}: Stderr of the process is closed.", id);
            }
        }
    }

    /**
     * StatusUpdater reads the process's stdout line by line. If the line is a JSON object, StatusUpdater updates
     * the status with the JSON object.
     */
    public class StatusUpdater implements Runnable {
        private Process process;
        StatusUpdater(Process process) {
            this.process = process;
        }
        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        JsonNode resp = JsonUtil.JSON_SERDE.readTree(line);
                        if (resp.has("status")) {
                            log.info("{}: Update the status: {}", id, resp.get("status").toString());
                            status.update(resp.get("status"));
                        }
                        if (resp.has("log")) {
                            log.info("{}: (stdout of the process): {}", id, resp.get("log").toString());
                        }
                        if (resp.has("error")) {
                            JsonNode errNode = resp.get("error");
                            if (errNode.getNodeType() == JsonNodeType.STRING) {
                                errMsg = errNode.asText();
                                log.error("{}: (stdout of the process):{}", id, errMsg);
                            }
                        }
                    } catch (IOException e) {
                        // not a JSON string
                    }
                }
            } catch (IOException ioe) {
                log.info("{}: Stdout f the process is closed.", id);
            }
        }
    }
}
