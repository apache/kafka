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
package org.apache.kafka.connect.mirror;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Scheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

    private final String name;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final Duration timeout;
    private boolean closed = false;

    Scheduler(String name, Duration timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    Scheduler(Class<?> clazz, String role, Duration timeout) {
        this("Scheduler for " + clazz.getSimpleName() + ": " + role, timeout);
    }

    void scheduleRepeating(Task task, Duration interval, String description) {
        if (interval.toMillis() < 0L) {
            return;
        }
        executor.scheduleAtFixedRate(() -> executeThread(task, description), 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }
 
    void scheduleRepeatingDelayed(Task task, Duration interval, String description) {
        if (interval.toMillis() < 0L) {
            return;
        }
        executor.scheduleAtFixedRate(() -> executeThread(task, description), interval.toMillis(),
            interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    void execute(Task task, String description) {
        try {
            executor.submit(() -> executeThread(task, description)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("{} was interrupted running task: {}", name, description);
        } catch (TimeoutException e) {
            LOG.error("{} timed out running task: {}", name, description);
        } catch (Throwable e) {
            LOG.error("{} caught exception in task: {}", name, description, e);
        }
    } 

    public void close() {
        closed = true;
        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!terminated) {
                LOG.error("{} timed out during shutdown of internal scheduler.", name);
            }
        } catch (InterruptedException e) {
            LOG.warn("{} was interrupted during shutdown of internal scheduler.", name);
        }
    }

    interface Task {
        void run() throws InterruptedException, ExecutionException;
    }

    private void run(Task task, String description) {
        try {
            long start = System.currentTimeMillis();
            task.run();
            long elapsed = System.currentTimeMillis() - start;
            LOG.info("{} took {} ms", description, elapsed);
            if (elapsed > timeout.toMillis()) {
                LOG.warn("{} took too long ({} ms) running task: {}", name, elapsed, description);
            }
        } catch (InterruptedException e) {
            LOG.warn("{} was interrupted running task: {}", name, description);
        } catch (Throwable e) {
            LOG.error("{} caught exception in scheduled task: {}", name, description, e);
        }
    }

    private void executeThread(Task task, String description) {
        Thread.currentThread().setName(name + "-" + description);
        if (closed) {
            LOG.info("{} skipping task due to shutdown: {}", name, description);
            return;
        }
        run(task, description);
    }
}

