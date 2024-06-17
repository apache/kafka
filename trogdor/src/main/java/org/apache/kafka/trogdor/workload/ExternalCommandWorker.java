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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;

/**
 * ExternalCommandWorker starts an external process to run a Trogdor command.
 *
 * The worker communicates with the external process over the standard input and output streams.
 *
 * When the process is first launched, ExternalCommandWorker will send a message on standard
 * input describing the task ID and the workload.  This message will not contain line breaks.
 * It will have this JSON format:
 * {"id":<task ID string>, "workload":<configured workload JSON object>}
 *
 * ExternalCommandWorker will log anything that the process writes to stderr, but will take
 * no other action with it.
 *
 * If the process sends a single-line JSON object to stdout, ExternalCommandWorker will parse it.
 * The JSON object can contain the following fields:
 * - status: If the object contains this field, the status will be set to the given value.
 * - error: If the object contains this field, the error will be set to the given value.
 *   Once an error occurs, we will try to terminate the process.
 * - log: If the object contains this field, a log message will be issued with this text.
 *
 * Note that standard output is buffered by default.  The subprocess may wish
 * to flush it after writing its status JSON.  This will ensure that the status
 * is seen in a timely fashion.
 *
 * If the process sends a non-JSON line to stdout, the worker will log it.
 *
 * If the process exits, ExternalCommandWorker will finish.  If the process exits unsuccessfully,
 * this is considered an error.  If the worker needs to stop the process, it will start by sending
 * a SIGTERM.  If this does not have the required effect, it will send a SIGKILL, once the shutdown
 * grace period has elapsed.
 */
public class ExternalCommandWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ExternalCommandWorker.class);

    private static final int DEFAULT_SHUTDOWN_GRACE_PERIOD_MS = 5000;

    /**
     * True only if the worker is running.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    enum TerminatorAction {
        DESTROY,
        DESTROY_FORCIBLY,
        CLOSE
    }

    /**
     * A queue used to communicate with the signal sender thread.
     */
    private final LinkedBlockingQueue<TerminatorAction> terminatorActionQueue = new LinkedBlockingQueue<>();

    /**
     * The queue of objects to write to the process stdin.
     */
    private final LinkedBlockingQueue<Optional<JsonNode>> stdinQueue = new LinkedBlockingQueue<>();

    /**
     * The task ID.
     */
    private final String id;

    /**
     * The command specification.
     */
    private final ExternalCommandSpec spec;

    /**
     * Tracks the worker status.
     */
    private WorkerStatusTracker status;

    /**
     * A future which should be completed when this worker is done.
     */
    private KafkaFutureImpl<String> doneFuture;

    /**
     * The executor service for this worker.
     */
    private ExecutorService executor;

    public ExternalCommandWorker(String id, ExternalCommandSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ConsumeBenchWorker is already running.");
        }
        log.info("{}: Activating ExternalCommandWorker with {}", id, spec);
        this.status = status;
        this.doneFuture = doneFuture;
        this.executor = Executors.newCachedThreadPool(
            ThreadUtils.createThreadFactory("ExternalCommandWorkerThread%d", false));
        Process process;
        try {
            process = startProcess();
        } catch (Throwable t) {
            log.error("{}: Unable to start process", id, t);
            executor.shutdown();
            doneFuture.complete("Unable to start process: " + t.getMessage());
            return;
        }
        Future<?> stdoutFuture = executor.submit(new StdoutMonitor(process));
        Future<?> stderrFuture = executor.submit(new StderrMonitor(process));
        executor.submit(new StdinWriter(process));
        Future<?> terminatorFuture = executor.submit(new Terminator(process));
        executor.submit(new ExitMonitor(process, stdoutFuture, stderrFuture, terminatorFuture));
        ObjectNode startMessage = new ObjectNode(JsonNodeFactory.instance);
        startMessage.set("id", new TextNode(id));
        startMessage.set("workload", spec.workload());
        stdinQueue.add(Optional.of(startMessage));
    }

    private Process startProcess() throws Exception {
        if (spec.command().isEmpty()) {
            throw new RuntimeException("No command specified");
        }
        ProcessBuilder bld = new ProcessBuilder(spec.command());
        return bld.start();
    }

    private static JsonNode readObject(String line) {
        JsonNode resp;
        try {
            resp = JsonUtil.JSON_SERDE.readTree(line);
        } catch (IOException e) {
            return NullNode.instance;
        }
        return resp;
    }

    class StdoutMonitor implements Runnable {
        private final Process process;

        StdoutMonitor(Process process) {
            this.process = process;
        }

        @Override
        public void run() {
            log.trace("{}: starting stdout monitor.", id);
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while (true) {
                    try {
                        line = br.readLine();
                        if (line == null) {
                            throw new IOException("EOF");
                        }
                    } catch (IOException e) {
                        log.info("{}: can't read any more from stdout: {}", id, e.getMessage());
                        return;
                    }
                    log.trace("{}: read line from stdin: {}", id, line);
                    JsonNode resp = readObject(line);
                    if (resp.has("status")) {
                        log.info("{}: New status: {}", id, resp.get("status").toString());
                        status.update(resp.get("status"));
                    }
                    if (resp.has("log")) {
                        log.info("{}: (stdout): {}", id, resp.get("log").asText());
                    }
                    if (resp.has("error")) {
                        String error = resp.get("error").asText();
                        log.error("{}: error: {}", id, error);
                        doneFuture.complete(error);
                    }
                }
            } catch (Throwable e) {
                log.info("{}: error reading from stdout.", id, e);
            }
        }
    }

    class StderrMonitor implements Runnable {
        private final Process process;

        StderrMonitor(Process process) {
            this.process = process;
        }

        @Override
        public void run() {
            log.trace("{}: starting stderr monitor.", id);
            try (BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while (true) {
                    try {
                        line = br.readLine();
                        if (line == null) {
                            throw new IOException("EOF");
                        }
                    } catch (IOException e) {
                        log.info("{}: can't read any more from stderr: {}", id, e.getMessage());
                        return;
                    }
                    log.error("{}: (stderr):{}", id, line);
                }
            } catch (Throwable e) {
                log.info("{}: error reading from stderr.", id, e);
            }
        }
    }

    class StdinWriter implements Runnable {
        private final Process process;

        StdinWriter(Process process) {
            this.process = process;
        }

        @Override
        public void run() {
            OutputStreamWriter stdinWriter = new OutputStreamWriter(
                process.getOutputStream(), StandardCharsets.UTF_8);
            try {
                while (true) {
                    log.info("{}: stdin writer ready.", id);
                    Optional<JsonNode> node = stdinQueue.take();
                    if (!node.isPresent()) {
                        log.trace("{}: StdinWriter terminating.", id);
                        return;
                    }
                    String inputString = JsonUtil.toJsonString(node.get());
                    log.info("{}: writing to stdin: {}", id, inputString);
                    stdinWriter.write(inputString + "\n");
                    stdinWriter.flush();
                }
            } catch (IOException e) {
                log.info("{}: can't write any more to stdin: {}", id, e.getMessage());
            } catch (Throwable e) {
                log.info("{}: error writing to stdin.", id, e);
            } finally {
                try {
                    stdinWriter.close();
                } catch (IOException e) {
                    log.debug("{}: error closing stdinWriter: {}", id, e.getMessage());
                }
            }
        }
    }

    class ExitMonitor implements Runnable {
        private final Process process;
        private final Future<?> stdoutFuture;
        private final Future<?> stderrFuture;
        private final Future<?> terminatorFuture;

        ExitMonitor(Process process, Future<?> stdoutFuture, Future<?> stderrFuture,
                    Future<?> terminatorFuture) {
            this.process = process;
            this.stdoutFuture = stdoutFuture;
            this.stderrFuture = stderrFuture;
            this.terminatorFuture = terminatorFuture;
        }

        @Override
        public void run() {
            try {
                int exitStatus = process.waitFor();
                log.info("{}: process exited with return code {}", id, exitStatus);
                // Wait for the stdout and stderr monitors to exit.  It's particularly important
                // to wait for the stdout monitor to exit since there may be an error or status
                // there that we haven't seen yet.
                stdoutFuture.get();
                stderrFuture.get();
                // Try to complete doneFuture with an error status based on the exit code.  Note
                // that if doneFuture was already completed previously, this will have no effect.
                if (exitStatus == 0) {
                    doneFuture.complete("");
                } else {
                    doneFuture.complete("exited with return code " + exitStatus);
                }
                // Tell the StdinWriter thread to exit.
                stdinQueue.add(Optional.empty());
                // Tell the shutdown manager thread to exit.
                terminatorActionQueue.add(TerminatorAction.CLOSE);
                terminatorFuture.get();
                executor.shutdown();
            } catch (Throwable e) {
                log.error("{}: ExitMonitor error", id, e);
                doneFuture.complete("ExitMonitor error: " + e.getMessage());
            }
        }
    }

    /**
     * The thread which manages terminating the child process.
     */
    class Terminator implements Runnable {
        private final Process process;

        Terminator(Process process) {
            this.process = process;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    switch (terminatorActionQueue.take()) {
                        case DESTROY:
                            log.info("{}: destroying process", id);
                            process.getInputStream().close();
                            process.getErrorStream().close();
                            process.destroy();
                            break;
                        case DESTROY_FORCIBLY:
                            log.info("{}: forcibly destroying process", id);
                            process.getInputStream().close();
                            process.getErrorStream().close();
                            process.destroyForcibly();
                            break;
                        case CLOSE:
                            log.trace("{}: closing Terminator thread.", id);
                            return;
                    }
                }
            } catch (Throwable e) {
                log.error("{}: Terminator error", id, e);
                doneFuture.complete("Terminator error: " + e.getMessage());
            }
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ExternalCommandWorker is not running.");
        }
        log.info("{}: Deactivating ExternalCommandWorker.", id);
        terminatorActionQueue.add(TerminatorAction.DESTROY);
        int shutdownGracePeriodMs = spec.shutdownGracePeriodMs().isPresent() ?
            spec.shutdownGracePeriodMs().get() : DEFAULT_SHUTDOWN_GRACE_PERIOD_MS;
        if (!executor.awaitTermination(shutdownGracePeriodMs, TimeUnit.MILLISECONDS)) {
            terminatorActionQueue.add(TerminatorAction.DESTROY_FORCIBLY);
            executor.awaitTermination(1, TimeUnit.DAYS);
        }
        this.status = null;
        this.doneFuture = null;
        this.executor = null;
    }
}
