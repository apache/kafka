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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * A handle to a worker executing in a Connect cluster.
 */
public class WorkerHandle {
    private static final Logger log = LoggerFactory.getLogger(WorkerHandle.class);

    private final String workerName;
    private final Connect worker;

    protected WorkerHandle(String workerName, Connect worker) {
        this.workerName = workerName;
        this.worker = worker;
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
     * Get the workers's name corresponding to this handle.
     *
     * @return the worker's name
     */
    public String name() {
        return workerName;
    }

    /**
     * Get the workers's url that accepts requests to its REST endpoint.
     *
     * @return the worker's url
     */
    public URI url() {
        return worker.restUrl();
    }

    @Override
    public String toString() {
        return "WorkerHandle{" +
                "workerName='" + workerName + '\'' +
                "workerURL='" + worker.restUrl() + '\'' +
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
