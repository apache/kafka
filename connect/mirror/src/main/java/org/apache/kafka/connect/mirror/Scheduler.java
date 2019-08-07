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
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Scheduler {
    private static Logger log = LoggerFactory.getLogger(Scheduler.class);

    private final String name;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    Scheduler(String name) {
        this.name = name;
    }

    Scheduler(Class clazz) {
        this("Scheduler for " + clazz.getSimpleName());
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

    void shutdown() {
        executor.shutdown();
    }

    void execute(Task task, String description) {
        try {
            long start = System.currentTimeMillis();
            task.run();
            long elapsed = System.currentTimeMillis() - start;
            log.info("{} took {} ms", description, elapsed);
        } catch (InterruptedException e) {
            log.warn("{} was interrupted running task: {}", name, description);
        } catch (Throwable e) {
            log.error("{} caught exception in scheduled task: {}", name, description, e);
        }
    }

    interface Task {
        void run() throws InterruptedException, ExecutionException;
    }

    private void executeThread(Task task, String description) {
        Thread.currentThread().setName(description);
        execute(task, description);
    }
}

