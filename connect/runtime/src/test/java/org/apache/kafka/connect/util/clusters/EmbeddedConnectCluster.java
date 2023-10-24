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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.PLUGIN_DISCOVERY_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.rest.RestServerConfig.LISTENERS_CONFIG;

/**
 * Start an embedded connect cluster. Internally, this class will spin up a Kafka and Zk cluster, set up any tmp
 * directories, and clean them up on exit. Methods on the same {@code EmbeddedConnectCluster} are
 * not guaranteed to be thread-safe.
 */
public class EmbeddedConnectCluster extends EmbeddedConnect {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectCluster.class);

    public static final int DEFAULT_NUM_WORKERS = 1;
    private static final String REST_HOST_NAME = "localhost";

    private static final String DEFAULT_WORKER_NAME_PREFIX = "connect-worker-";

    private final Set<WorkerHandle> connectCluster;
    private final Map<String, String> workerProps;
    private final String connectClusterName;
    private final int numInitialWorkers;
    private final String workerNamePrefix;
    private final AtomicInteger nextWorkerId = new AtomicInteger(0);

    private EmbeddedConnectCluster(
            int numBrokers,
            Properties brokerProps,
            boolean maskExitProcedures,
            Map<String, String> clientProps,
            Map<String, String> workerProps,
            String name,
            int numWorkers
    ) {
        super(numBrokers, brokerProps, maskExitProcedures, clientProps);
        this.workerProps = workerProps;
        this.connectClusterName = name;
        this.connectCluster = new LinkedHashSet<>();
        this.numInitialWorkers = numWorkers;
        // leaving non-configurable for now
        this.workerNamePrefix = DEFAULT_WORKER_NAME_PREFIX;
    }

    /**
     * Provision and start an additional worker to the Connect cluster.
     *
     * @return the worker handle of the worker that was provisioned
     */
    public WorkerHandle addWorker() {
        WorkerHandle worker = WorkerHandle.start(workerNamePrefix + nextWorkerId.getAndIncrement(), workerProps);
        connectCluster.add(worker);
        log.info("Started worker {}", worker);
        return worker;
    }

    /**
     * Decommission one of the workers from this Connect cluster. Which worker is removed is
     * implementation dependent and selection is not guaranteed to be consistent. Use this method
     * when you don't care which worker stops.
     *
     * @see #removeWorker(WorkerHandle)
     */
    public void removeWorker() {
        WorkerHandle toRemove = null;
        for (Iterator<WorkerHandle> it = connectCluster.iterator(); it.hasNext(); toRemove = it.next()) {
        }
        if (toRemove != null) {
            removeWorker(toRemove);
        }
    }

    /**
     * Decommission a specific worker from this Connect cluster.
     *
     * @param worker the handle of the worker to remove from the cluster
     * @throws IllegalStateException if the Connect cluster has no workers
     */
    public void removeWorker(WorkerHandle worker) {
        if (connectCluster.isEmpty()) {
            throw new IllegalStateException("Cannot remove worker. Cluster is empty");
        }
        stopWorker(worker);
        connectCluster.remove(worker);
    }

    /**
     * Determine whether the Connect cluster has any workers running.
     *
     * @return true if any worker is running, or false otherwise
     */
    public boolean anyWorkersRunning() {
        return workers().stream().anyMatch(WorkerHandle::isRunning);
    }

    /**
     * Determine whether the Connect cluster has all workers running.
     *
     * @return true if all workers are running, or false otherwise
     */
    public boolean allWorkersRunning() {
        return workers().stream().allMatch(WorkerHandle::isRunning);
    }

    @Override
    public void startConnect() {
        log.info("Starting Connect cluster '{}' with {} workers", connectClusterName, numInitialWorkers);

        workerProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka().bootstrapServers());
        // use a random available port
        workerProps.put(LISTENERS_CONFIG, "HTTP://" + REST_HOST_NAME + ":0");

        String internalTopicsReplFactor = String.valueOf(numBrokers);
        workerProps.putIfAbsent(GROUP_ID_CONFIG, "connect-integration-test-" + connectClusterName);
        workerProps.putIfAbsent(OFFSET_STORAGE_TOPIC_CONFIG, "connect-offset-topic-" + connectClusterName);
        workerProps.putIfAbsent(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, internalTopicsReplFactor);
        workerProps.putIfAbsent(CONFIG_TOPIC_CONFIG, "connect-config-topic-" + connectClusterName);
        workerProps.putIfAbsent(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, internalTopicsReplFactor);
        workerProps.putIfAbsent(STATUS_STORAGE_TOPIC_CONFIG, "connect-status-topic-" + connectClusterName);
        workerProps.putIfAbsent(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, internalTopicsReplFactor);
        workerProps.putIfAbsent(KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.putIfAbsent(VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.putIfAbsent(PLUGIN_DISCOVERY_CONFIG, "hybrid_fail");

        for (int i = 0; i < numInitialWorkers; i++) {
            addWorker();
        }
    }

    @Override
    public String toString() {
        return String.format("EmbeddedConnectCluster(name= %s, numBrokers= %d, numInitialWorkers= %d, workerProps= %s)",
            connectClusterName,
            numBrokers,
            numInitialWorkers,
            workerProps);
    }

    public String getName() {
        return connectClusterName;
    }

    /**
     * Get the provisioned workers.
     *
     * @return the list of handles of the provisioned workers
     */
    public Set<WorkerHandle> workers() {
        return new LinkedHashSet<>(connectCluster);
    }

    public static class Builder extends EmbeddedConnectBuilder<EmbeddedConnectCluster, Builder> {
        private String name = UUID.randomUUID().toString();
        private int numWorkers = DEFAULT_NUM_WORKERS;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder numWorkers(int numWorkers) {
            this.numWorkers = numWorkers;
            return this;
        }

        /**
         * @deprecated Use {@link #clientProps(Map)} instead.
         */
        @Deprecated
        public Builder clientConfigs(Map<String, String> clientProps) {
            return clientProps(clientProps);
        }

        @Override
        protected EmbeddedConnectCluster build(
                int numBrokers,
                Properties brokerProps,
                boolean maskExitProcedures,
                Map<String, String> clientProps,
                Map<String, String> workerProps
        ) {
            return new EmbeddedConnectCluster(
                    numBrokers,
                    brokerProps,
                    maskExitProcedures,
                    clientProps,
                    workerProps,
                    name,
                    numWorkers
            );
        }
    }

}
