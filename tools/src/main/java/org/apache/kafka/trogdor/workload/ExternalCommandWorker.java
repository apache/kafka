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
import org.apache.kafka.trogdor.common.WorkerUtils;
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

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
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
 * Stop a task {"action":"stop", "spec":ExternalCommandSpec}.
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

    private ScheduledExecutorService executor;

    private Process process;

    private PrintWriter controlChannel;

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
        this.executor = Executors.newScheduledThreadPool(3, // Prepare, updater and errorUpdater
            ThreadUtils.createThreadFactory("ExternalCommandWorkerThread%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        executor.submit(new ProcessMonitor());
    }

    public class ProcessMonitor implements Runnable {
        @Override
        public void run() {
            try {
                if (spec.command().isEmpty()) {
                    errMsg = "Empty Command";
                    doneFuture.complete(errMsg);
                    return;
                }
                ProcessBuilder processBuilder = new ProcessBuilder(spec.command());
                process = processBuilder.start();
                Future updater = executor.submit(new StatusUpdater());
                Future errorUpdater = executor.submit(new ErrorUpdater());
                controlChannel = new PrintWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8), true);
                new ControlCommand("start", spec).issue();
                log.info("{}: Started the process {} to execute command: {}", id, process.toString(), spec.command());
                process.waitFor();
                updater.get();
                errorUpdater.get();
                int workerExitValue = process.exitValue();
                log.info("{}: The process finished with the exit value: {}.", id, workerExitValue);
                if (workerExitValue != 0 && errMsg.isEmpty()) {
                    errMsg = "ExternalWorker exited with error code " + process.exitValue();
                }
                log.info("{}: StatusUpdater is terminated.", id);
                doneFuture.complete(errMsg);
            } catch (IOException e) {
                log.info("{}: Stdout of the process is closed.", id);
            } catch (InterruptedException e) {
                log.info("{}: StatusUpdater is interrupted.", id);
            } catch (Exception e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            } finally {
                if (process != null && process.isAlive()) {
                    process.destroy();
                }
            }
        }
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

        public void issue() {
            String command = JsonUtil.toJsonString(this);
            log.info("{}: Send out command: {} ", id, command);
            controlChannel.println(command);
        }
    }

    public class ErrorUpdater implements Runnable {
        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    log.error("{}: (stderr of the process):{}", id, line);
                    errMsg = line;
                }
            } catch (IOException ioe) {
                log.info("{}: Stderr of the process is closed.", id);
            } catch (Exception e) {
                WorkerUtils.abort(log, "ErrorUpdater", e, doneFuture);
            }
        }
    }

    /**
     * StatusUpdater reads ProcessWorker's stdout line by line. If the line is a JSON object, StatusUpdater updates
     * the status with the JSON object.
     */
    public class StatusUpdater implements Runnable {
        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        JsonNode resp = JsonUtil.JSON_SERDE.readTree(line);
                        if (resp.has("status")) {
                            status.update(resp.get("status"));
                        }
                        if (resp.has("log")) {
                            log.info("{}: (stdout of the process): {}", id, resp.get("log").toString());
                        }
                        if (resp.has("error")) {
                            JsonNode errNode = resp.get("error");
                            if (errNode.getNodeType() == JsonNodeType.STRING) {
                                errMsg = errNode.asText();
                            }
                        }
                    } catch (IOException e) {
                        // not a JSON string
                    }
                }
            } catch (IOException ioe) {
                log.info("{}: Stdout f the process is closed.", id);
            } catch (Exception e) {
                WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
            }
        }
    }


    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ExternalCommandWorker is not running.");
        }
        log.info("{}: Deactivating ExternalCommandWorker.", id);
        if (process != null) {
            if (process.isAlive()) {
                new ControlCommand("stop", spec).issue();
            }
            process.waitFor(1, TimeUnit.MINUTES);
            if (process.isAlive()) {
                log.info("{}: Destroy the external process since it is still alive after 1 minute.");
                process.destroy();
            }
        }
        doneFuture.complete(errMsg);
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        this.executor = null;
        this.process = null;
        this.status = null;
        this.doneFuture = null;
    }
}
