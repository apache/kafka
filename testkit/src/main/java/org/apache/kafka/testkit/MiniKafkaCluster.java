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
package org.apache.kafka.testkit;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implements a test cluster.
 */
public class MiniKafkaCluster implements AutoCloseable {
    private final Logger log;
    private Map<Integer, MiniKafkaNode> kafkas = new TreeMap<>();
    private Map<String, MiniZookeeperNode> zookeepers = new HashMap<>();

    MiniKafkaCluster(MiniKafkaClusterBuilder clusterBld) {
        this.log = clusterBld.logContext.logger(MiniKafkaCluster.class);
        boolean successful = false, interrupted = false;
        ExecutorService executorService = Executors.newFixedThreadPool(clusterBld.kafkaBlds.size());
        try {
            // Initialize ZK nodes
            int zkIndex = 0;
            log.trace("Starting {} zookeeper(s).", clusterBld.zkBlds.size());
            for (MiniZookeeperNodeBuilder zkBld : clusterBld.zkBlds) {
                MiniZookeeperNode zookeeper = zkBld.build(clusterBld, String.format("zk%d", zkIndex++));
                zookeeper.start();
                this.zookeepers.put(zookeeper.hostPort(), zookeeper);
            }
            log.trace("Finished starting {} zookeeper(s).  Starting {} kafka(s).",
                clusterBld.zkBlds.size(), clusterBld.kafkaBlds.size());

            // Initialize Kafka nodes
            int autoAssignedIdx = 0;
            for (MiniKafkaNodeBuilder kafkaBld : clusterBld.kafkaBlds) {
                final String brokerName;
                if (kafkaBld.id == MiniKafkaNodeBuilder.INVALID_NODE_ID) {
                    brokerName = generateNameForAutoAssignedBroker(autoAssignedIdx++);
                } else {
                    if (kafkas.containsKey(kafkaBld.id)) {
                        throw new RuntimeException("Can't have two brokers both assigned node id " +
                            kafkaBld.id);
                    }
                    brokerName = String.format("broker%d", kafkaBld.id);
                }
                final MiniKafkaNode kafka = kafkaBld.build(clusterBld, brokerName, zkString());
                executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                kafka.start();
                                kafkas.put(kafka.id(), kafka);
                            } catch (Throwable e) {
                                log.error("Unable to start {}", brokerName, e);
                                kafka.shutdown();
                            }
                        }
                    });
            }
            executorService.shutdown();
            interrupted = TestKitUtil.awaitTerminationUninterruptibly(executorService);
            if (kafkas.size() != clusterBld.kafkaBlds.size()) {
                throw new RuntimeException("Broker(s) failed to start.");
            }
            successful = true;
            log.trace("Finished starting {} kafka(s). Cluster setup is complete.",
                clusterBld.kafkaBlds.size());
        } finally {
            if (!successful) {
                executorService.shutdownNow();
                close();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public String zkString() {
        return Utils.join(zookeepers.keySet(), ",");
    }

    /**
     * Generate a name for an auto-assigned broker.
     */
    private final static String generateNameForAutoAssignedBroker(int index) {
        if (index < ('Z' - 'A')) {
            return String.format("broker%c", 'A' + index);
        }
        int count = index / ('Z' - 'A');
        index %= 'Z' - 'A';
        return String.format("broker%d%c", count, 'A' + index);
    }

    public Map<Integer, MiniKafkaNode> kafkas() {
        return Collections.unmodifiableMap(this.kafkas);
    }

    public Map<String, MiniZookeeperNode> zookeepers() {
        return Collections.unmodifiableMap(this.zookeepers);
    }

    @Override
    public void close() {
        boolean interrupted = false;
        log.trace("Closing cluster.  Shutting down {} kafka(s).", kafkas.size());
        if (!kafkas.isEmpty()) {
            ExecutorService executorService = Executors.newFixedThreadPool(this.kafkas.size());
            for (final MiniKafkaNode kafka : this.kafkas.values()) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        kafka.shutdown();
                    }
                });
            }
            executorService.shutdown();
            interrupted |= TestKitUtil.awaitTerminationUninterruptibly(executorService);
        }

        log.trace("Finished shutting down {} kafka server(s). Shutting down {} zookeeper(s).",
            kafkas.size(), zookeepers.size());
        for (MiniZookeeperNode zookeeper : zookeepers.values()) {
            zookeeper.shutdown();
        }
        log.trace("Finished shutting down {} zookeeper server(s). Starting {} kafka(s).");
        for (MiniKafkaNode kafka : kafkas.values()) {
            kafka.close();
        }
        log.trace("Finished closing {} kafka(s). Closing {} zookeeper(s).", kafkas.size());
        kafkas.clear();
        for (MiniZookeeperNode zookeeper : zookeepers.values()) {
            zookeeper.close();
        }
        log.trace("Finished closing {} zookeeper(s).  Finished closing cluster.", zookeepers.size());
        zookeepers.clear();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
