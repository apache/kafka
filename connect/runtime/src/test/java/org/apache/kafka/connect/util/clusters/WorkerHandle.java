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
package org.apache.kafka.connect.util.clusters;

import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.rest.RestServer;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * A handle to a worker executing in a Connect cluster.
 */
public class WorkerHandle {

    private final String workerName;
    private final Connect<?> worker;

    protected WorkerHandle(String workerName, Connect<?> worker) {
        this.workerName = workerName;
        this.worker = worker;
    }

    /**
     * Track the worker status during startup.
     * @return {@link Connect#herderTask()} to track or null
     */
    public Future<?> herderTask() {
        return worker.herderTask();
    }

    /**
     * Create and start a new worker with the given properties.
     *
     * @param name a name for this worker
     * @param workerProperties the worker properties
     * @return the worker's handle
     */
    public static WorkerHandle start(String name, Map<String, String> workerProperties) {
        return new WorkerHandle(name, new ConnectDistributed().startConnect(workerProperties));
    }

    /**
     * Stop this worker.
     */
    public void stop() {
        worker.stop();
    }

    /**
     * Get the workers's url that accepts requests to its REST endpoint.
     *
     * @return the worker's url
     */
    public URI url() {
        return worker.rest().serverUrl();
    }

    /**
     * Get the workers's url that accepts requests to its Admin REST endpoint.
     *
     * @return the worker's admin url
     */
    public URI adminUrl() {
        return worker.rest().adminUrl();
    }

    /**
     * Set a new timeout for REST requests to the worker, including health check requests.
     * Useful if a request is expected to block, since the time spent awaiting that request
     * can be reduced and test runtime bloat can be avoided.
     * @param timeoutMs the new timeout in milliseconds; must be positive
     */
    public void requestTimeout(long timeoutMs) {
        worker.rest().requestTimeout(timeoutMs);
        worker.rest().healthCheckTimeout(timeoutMs);
    }

    /**
     * Reset the timeout for REST requests to the worker, including health check requests.
     */
    public void resetRequestTimeout() {
        worker.rest().requestTimeout(RestServer.DEFAULT_REST_REQUEST_TIMEOUT_MS);
        worker.rest().healthCheckTimeout(RestServer.DEFAULT_HEALTH_CHECK_TIMEOUT_MS);
    }

    @Override
    public String toString() {
        return "WorkerHandle{" +
                "workerName='" + workerName + '\'' +
                "workerURL='" + worker.rest().serverUrl() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WorkerHandle)) {
            return false;
        }
        WorkerHandle that = (WorkerHandle) o;
        return Objects.equals(workerName, that.workerName) &&
                Objects.equals(worker, that.worker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerName, worker);
    }
}
