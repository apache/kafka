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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionStressWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ConnectionStressWorker.class);
    private static final Time TIME = Time.SYSTEM;

    private static final int THROTTLE_PERIOD_MS = 100;

    private static final int REPORT_INTERVAL_MS = 20000;

    private final String id;

    private final ConnectionStressSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaFutureImpl<String> doneFuture;

    private WorkerStatusTracker status;

    private long totalConnections;

    private long totalFailedConnections;

    private long startTimeMs;

    private Throttle throttle;

    private long nextReportTime;

    private ExecutorService workerExecutor;

    public ConnectionStressWorker(String id, ConnectionStressSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ConnectionStressWorker is already running.");
        }
        log.info("{}: Activating ConnectionStressWorker with {}", id, spec);
        this.doneFuture = doneFuture;
        this.status = status;
        this.totalConnections = 0;
        this.totalFailedConnections  = 0;
        this.startTimeMs = TIME.milliseconds();
        this.throttle = new ConnectStressThrottle(WorkerUtils.
            perSecToPerPeriod(spec.targetConnectionsPerSec(), THROTTLE_PERIOD_MS));
        this.nextReportTime = 0;
        this.workerExecutor = Executors.newFixedThreadPool(spec.numThreads(),
            ThreadUtils.createThreadFactory("ConnectionStressWorkerThread%d", false));
        for (int i = 0; i < spec.numThreads(); i++) {
            this.workerExecutor.submit(new ConnectLoop());
        }
    }

    private static class ConnectStressThrottle extends Throttle {
        ConnectStressThrottle(int maxPerPeriod) {
            super(maxPerPeriod, THROTTLE_PERIOD_MS);
        }
    }

    public class ConnectLoop implements Runnable {
        @Override
        public void run() {
            try {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
                WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.commonClientConf());
                AdminClientConfig conf = new AdminClientConfig(props);
                List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                        conf.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                        conf.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG));
                ManualMetadataUpdater updater = new ManualMetadataUpdater(Cluster.bootstrap(addresses).nodes());
                while (true) {
                    if (doneFuture.isDone()) {
                        break;
                    }
                    throttle.increment();
                    long lastTimeMs = throttle.lastTimeMs();
                    boolean success = false;
                    switch (spec.action()) {
                        case CONNECT:
                            success = attemptConnection(conf, updater);
                            break;
                        case FETCH_METADATA:
                            success = attemptMetadataFetch(props);
                            break;
                    }
                    synchronized (ConnectionStressWorker.this) {
                        totalConnections++;
                        if (!success) {
                            totalFailedConnections++;
                        }
                        if (lastTimeMs > nextReportTime) {
                            status.update(JsonUtil.JSON_SERDE.valueToTree(
                                new StatusData(totalConnections,
                                    totalFailedConnections,
                                    (totalConnections * 1000.0) / (lastTimeMs - startTimeMs))));
                            nextReportTime = lastTimeMs + REPORT_INTERVAL_MS;
                        }
                    }
                }
            } catch (Exception e) {
                WorkerUtils.abort(log, "ConnectionStressRunnable", e, doneFuture);
            }
        }

        private boolean attemptConnection(AdminClientConfig conf,
                                          ManualMetadataUpdater updater) throws Exception {
            try {
                List<Node> nodes = updater.fetchNodes();
                Node targetNode = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
                try (ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(conf, TIME)) {
                    try (Metrics metrics = new Metrics()) {
                        LogContext logContext = new LogContext();
                        try (Selector selector = new Selector(conf.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                            metrics, TIME, "", channelBuilder, logContext)) {
                            try (NetworkClient client = new NetworkClient(selector,
                                    updater,
                                    "ConnectionStressWorker",
                                    1,
                                    1000,
                                    1000,
                                    4096,
                                    4096,
                                    1000,
                                    ClientDnsLookup.forConfig(conf.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                                    TIME,
                                    false,
                                    new ApiVersions(),
                                    logContext)) {
                                NetworkClientUtils.awaitReady(client, targetNode, TIME, 100);
                            }
                        }
                    }
                }
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        private boolean attemptMetadataFetch(Properties conf) {
            try (AdminClient client = AdminClient.create(conf)) {
                client.describeCluster().nodes().get();
            } catch (RuntimeException e) {
                return false;
            } catch (Exception e) {
                return false;
            }
            return true;
        }
    }

    public static class StatusData {
        private final long totalConnections;
        private final long totalFailedConnections;
        private final double connectsPerSec;

        @JsonCreator
        StatusData(@JsonProperty("totalConnections") long totalConnections,
                   @JsonProperty("totalFailedConnections") long totalFailedConnections,
                   @JsonProperty("connectsPerSec") double connectsPerSec) {
            this.totalConnections = totalConnections;
            this.totalFailedConnections = totalFailedConnections;
            this.connectsPerSec = connectsPerSec;
        }

        @JsonProperty
        public long totalConnections() {
            return totalConnections;
        }

        @JsonProperty
        public long totalFailedConnections() {
            return totalFailedConnections;
        }

        @JsonProperty
        public double connectsPerSec() {
            return connectsPerSec;
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ConnectionStressWorker is not running.");
        }
        log.info("{}: Deactivating ConnectionStressWorker.", id);
        doneFuture.complete("");
        workerExecutor.shutdownNow();
        workerExecutor.awaitTermination(1, TimeUnit.DAYS);
        this.workerExecutor = null;
        this.status = null;
    }
}
