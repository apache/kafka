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
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.TestSinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A connector to be used in integration tests. This class provides methods to find task instances
 * which are initiated by the embedded connector, and wait for them to consume a desired number of
 * messages.
 */
public class MonitorableSinkConnector extends TestSinkConnector {

    public static final String EXPECTED_RECORDS = "expected_records";
    private static final Logger log = LoggerFactory.getLogger(MonitorableSinkConnector.class);

    private static final Map<String, Handle> HANDLES = new ConcurrentHashMap<>();

    private String connectorName;
    private String expectedRecordsStr;

    public static class Handle {
        private static final int MAX_WAIT_FOR_TASK_DURATION_MS = 60_000;

        private final String taskId;
        private final AtomicReference<MonitorableSinkTask> taskRef = new AtomicReference<>();
        //  a value of 1 in the latch means that there is no task available.
        private final CountDownLatch taskAvailable = new CountDownLatch(1);

        public Handle(String taskId) {
            this.taskId = taskId;
        }

        public void task(MonitorableSinkTask task) {
            if (this.taskRef.compareAndSet(null, task)) {
                // we have a task, set latch to zero.
                taskAvailable.countDown();
            }
        }

        public MonitorableSinkTask task() {
            try {
                log.debug("Waiting on task {}", taskId);
                // wait for limited duration for a task to be initialized and set to taskRef,
                // if we already have a task (i.e., taskAvailable is set to zero), await will immediately return
                if (!taskAvailable.await(MAX_WAIT_FOR_TASK_DURATION_MS, TimeUnit.MILLISECONDS)) {
                    throw new ConnectException("Could not find task '" + taskId + "'.");
                }
                log.debug("Found task!");
            } catch (InterruptedException e) {
                throw new ConnectException("Could not find task for " + taskId, e);
            }

            return taskRef.get();
        }

        @Override
        public String toString() {
            return "Handle{" +
                    "taskId='" + taskId + '\'' +
                    ", taskAvailable=" + (taskAvailable.getCount() == 0) +
                    '}';
        }
    }

    public static MonitorableSinkTask task(String taskId) {
        return HANDLES.computeIfAbsent(taskId, connName -> new Handle(taskId)).task();
    }

    public static void task(String taskId, MonitorableSinkTask task) {
        HANDLES.computeIfAbsent(taskId, connName -> new Handle(taskId)).task(task);
    }

    public static void cleanHandle(String taskId) {
        HANDLES.computeIfPresent(taskId, (k, handle) -> {
            // unblock any waiting threads
            handle.taskAvailable.countDown();
            // remove this key from HANDLES
            return null;
        });
    }

    @Override
    public void start(Map<String, String> props) {
        connectorName = props.get("name");
        expectedRecordsStr = props.get(EXPECTED_RECORDS);
        log.info("Starting connector {}", props.get("name"));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MonitorableSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("task.id", connectorName + "-" + i);
            config.put(EXPECTED_RECORDS, expectedRecordsStr);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    public static class MonitorableSinkTask extends SinkTask {

        private String taskId;
        private int expectedRecords;
        private CountDownLatch latch;

        @Override
        public String version() {
            return "unknown";
        }

        @Override
        public void start(Map<String, String> props) {
            log.debug("Starting task {}", context);
            taskId = props.get("task.id");
            expectedRecords = Integer.parseInt(props.get(EXPECTED_RECORDS));
            task(taskId, this);
            latch = new CountDownLatch(expectedRecords);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord rec : records) {
                latch.countDown();
                log.debug("Obtained record (key='{}' value='{}') at task {}", rec.key(), rec.value(), context);
            }
        }

        @Override
        public void stop() {
            cleanHandle(taskId);
            log.info("Removing handle for taskId {}", taskId);
        }

        public void awaitRecords(int consumeMaxDurationMs) throws InterruptedException {
            if (latch == null) {
                throw new IllegalStateException("Illegal state encountered. Maybe this task was not started by the framework?");
            } else {
                if (!latch.await(consumeMaxDurationMs, TimeUnit.MILLISECONDS)) {
                    String msg = String.format("Insufficient records seen by task %s in %d millis. Records expected=%d, actual=%d",
                            taskId,
                            consumeMaxDurationMs,
                            expectedRecords,
                            expectedRecords - latch.getCount());
                    throw new DataException(msg);
                }
            }
        }
    }
}
