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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RuntimeProcessWorker starts a worker process by executing spec.workerCommand --spec  SPEC_STRING.
 * It monitors both stdout and stderr of the worker process line by line. For any stdout line that is a valid
 * JSON string, it updates the RuntimeProcessWorker's status with that JSON value.
 */
public class RuntimeProcessWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ProduceBenchWorker.class);

    private final String id;

    private final TaskSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledExecutorService executor;

    private Process worker;

    private WorkerStatusTracker status;

    private KafkaFutureImpl<String> doneFuture;

    public RuntimeProcessWorker(String id, TaskSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("RuntimeProcessWorker is already running.");
        }
        log.info("{}: Activating RuntimeProcessWorker with {}", id, spec);
        this.executor = Executors.newScheduledThreadPool(2,
            ThreadUtils.createThreadFactory("RuntimeProcessWorkerThread%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        executor.submit(new Prepare());
    }

    public class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                ArrayList<String> command = new ArrayList<String>(spec.workerCommand());
                command.add("--spec");
                command.add(spec.toString());
                log.info("RuntimeProcessWorker executes command: {}", command);
                ProcessBuilder workerBuilder = new ProcessBuilder(command);
                // Start the worker, StatusUpdater monitors its stdout and ErrorUpdater monitors its stderr.
                worker = workerBuilder.start();
                Future updater = executor.submit(new StatusUpdater());
                executor.submit(new ErrorUpdater());
                worker.waitFor();
                if (worker.exitValue() != 0) {
                    status.update(new TextNode("Error: " + command + " returns " + worker.exitValue()));
                }
                log.info("ProcessWorker finished with the exit value:{}.", worker.exitValue());
                updater.get();
                log.info("StatusUpdater finished.");
                doneFuture.complete("");
            } catch (Throwable e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            }
        }
    }

    public class ErrorUpdater implements Runnable {
        BufferedReader br;
        ErrorUpdater() {
            br = new BufferedReader(new InputStreamReader(worker.getErrorStream(), StandardCharsets.UTF_8));
        }
        @Override
        public void run() {
            try {
                String line;
                while ((line = br.readLine()) != null) {
                    log.error("Worker (stderr):{}", line);
                }
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

        BufferedReader br;
        StatusUpdater() {
            br = new BufferedReader(new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8));
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = br.readLine()) != null) {
                    // if this is a JSON string
                    log.info("Worker (stdout):{}", line);
                    try {
                        JsonNode resp = JsonUtil.JSON_SERDE.readTree(line);
                        status.update(resp);
                    } catch (IOException e) {
                        // not a JSON string
                    }
                }
            } catch (Exception e) {
                WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
            }
        }
    }


    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ProduceBenchWorker is not running.");
        }
        log.info("{}: Deactivating ProduceBenchWorker.", id);
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        worker.waitFor(1, TimeUnit.MINUTES);
        this.executor = null;
        this.worker = null;
        this.status = null;
        this.doneFuture = null;
        doneFuture.complete("");
    }
}
