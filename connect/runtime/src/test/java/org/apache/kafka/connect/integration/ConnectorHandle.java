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

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A handle to a connector executing in a Connect cluster.
 */
public class ConnectorHandle {

    private static final Logger log = LoggerFactory.getLogger(ConnectorHandle.class);

    private final String connectorName;
    private final Map<String, TaskHandle> taskHandles = new ConcurrentHashMap<>();

    private CountDownLatch recordsRemainingLatch;
    private int expectedRecords = -1;

    public ConnectorHandle(String connectorName) {
        this.connectorName = connectorName;
    }

    /**
     * Get or create a task handle for a given task id. The task need not be created when this method is called. If the
     * handle is called before the task is created, the task will bind to the handle once it starts (or restarts).
     *
     * @param taskId the task id
     * @return a non-null {@link TaskHandle}
     */
    public TaskHandle taskHandle(String taskId) {
        return taskHandles.computeIfAbsent(taskId, k -> new TaskHandle(this, taskId));
    }

    public Collection<TaskHandle> tasks() {
        return taskHandles.values();
    }

    /**
     * Delete the task handle for this task id.
     *
     * @param taskId the task id.
     */
    public void deleteTask(String taskId) {
        log.info("Removing handle for {} task in connector {}", taskId, connectorName);
        taskHandles.remove(taskId);
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param expectedRecords number of records
     */
    public void expectedRecords(int expectedRecords) {
        this.expectedRecords = expectedRecords;
        this.recordsRemainingLatch = new CountDownLatch(expectedRecords);
    }

    /**
     * Record a message arrival at the connector.
     */
    public void record() {
        if (recordsRemainingLatch != null) {
            recordsRemainingLatch.countDown();
        }
    }

    /**
     * Wait for this task to receive the expected number of records.
     *
     * @param consumeMaxDurationMs max duration to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(int consumeMaxDurationMs) throws InterruptedException {
        if (recordsRemainingLatch == null || expectedRecords < 0) {
            throw new IllegalStateException("expectedRecords() was not set for this task?");
        }
        if (!recordsRemainingLatch.await(consumeMaxDurationMs, TimeUnit.MILLISECONDS)) {
            String msg = String.format("Insufficient records seen by connector %s in %d millis. Records expected=%d, actual=%d",
                    connectorName,
                    consumeMaxDurationMs,
                    expectedRecords,
                    expectedRecords - recordsRemainingLatch.getCount());
            throw new DataException(msg);
        }
    }

    @Override
    public String toString() {
        return "ConnectorHandle{" +
                "connectorName='" + connectorName + '\'' +
                '}';
    }
}
