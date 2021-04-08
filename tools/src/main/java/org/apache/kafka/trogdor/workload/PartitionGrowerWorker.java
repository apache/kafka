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
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionGrowerWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(PartitionGrowerWorker.class);
    private static final Time TIME = Time.SYSTEM;
    private static final int REPORT_INTERVAL_MS = 5000;

    private final String id;

    private final PartitionGrowerSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaFutureImpl<String> doneFuture;

    private WorkerStatusTracker status;

    private int currentPartitionCount;
    private boolean lastAttemptSuccessful;
    private String lastError;

    private long startTimeMs;

    private Future<?> statusUpdaterFuture;

    private ScheduledExecutorService executor;

    PartitionGrowerWorker(String id, PartitionGrowerSpec spec) {
        this.id = id;
        this.spec = spec;
        this.lastAttemptSuccessful = false;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("PartitionGrowerWorker is already running.");
        }
        log.info("{}: Activating PartitionGrowerWorker with {}", id, spec);
        this.doneFuture = doneFuture;
        this.status = status;
        synchronized (PartitionGrowerWorker.this) {
            this.startTimeMs = TIME.milliseconds();
            this.currentPartitionCount = 0;
        }
        this.executor = Executors.newScheduledThreadPool(2,
            ThreadUtils.createThreadFactory("WorkerThread%d", false));
        this.statusUpdaterFuture = this.executor.scheduleAtFixedRate(
            new StatusUpdater(), 0, REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        this.executor.scheduleAtFixedRate(
            new AddPartitionsToExistingTopic(), 0, this.spec.growthIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public class AddPartitionsToExistingTopic implements Runnable {
        private final Properties props;

        AddPartitionsToExistingTopic() {
            this.props = new Properties();
            this.props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PartitionGrowerWorker.this.spec.bootstrapServers());
            WorkerUtils.addConfigsToProperties(
                this.props, PartitionGrowerWorker.this.spec.commonClientConf(),
                PartitionGrowerWorker.this.spec.commonClientConf());

        }
        @Override
        public void run() {
            String topicName = PartitionGrowerWorker.this.spec.topicName();
            try {
                // Create the admin client and figure out the current partitions, if necessary.
                AdminClient client = AdminClient.create(this.props);
                if (PartitionGrowerWorker.this.currentPartitionCount == 0) {
                    Collection<String> topics = Collections.singleton(topicName);
                    Map<String, TopicDescription> topicDetails = client.describeTopics(topics).all().get();
                    PartitionGrowerWorker.this.currentPartitionCount = topicDetails.get(topicName).partitions().size();
                }

                // Increase the partitions.
                PartitionGrowerWorker.this.currentPartitionCount += PartitionGrowerWorker.this.spec.growPartitions();
                NewPartitions np = NewPartitions.increaseTo(PartitionGrowerWorker.this.currentPartitionCount);
                HashMap<String, NewPartitions> topicNewPartitions = new HashMap<>();
                topicNewPartitions.put(topicName, np);
                client.createPartitions(topicNewPartitions).all().get();

                // Close and set state.
                Utils.closeQuietly(client, "AdminClient");
                PartitionGrowerWorker.this.lastAttemptSuccessful = true;
                PartitionGrowerWorker.this.lastError = "";
            } catch (Exception e) {
                PartitionGrowerWorker.this.lastAttemptSuccessful = false;
                PartitionGrowerWorker.this.lastError = e.getLocalizedMessage();
            }
        }
    }

    private class StatusUpdater implements Runnable {
        @Override
        public void run() {
            try {
                JsonNode node = null;
                synchronized (PartitionGrowerWorker.this) {
                    node = JsonUtil.JSON_SERDE.valueToTree(
                        new StatusData(PartitionGrowerWorker.this.currentPartitionCount,
                                       PartitionGrowerWorker.this.lastAttemptSuccessful,
                                       PartitionGrowerWorker.this.lastError));
                }
                status.update(node);
            } catch (Exception e) {
                WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
            }
        }
    }

    public static class StatusData {
        private final long currentPartitions;
        private final boolean lastAttemptSuccessful;
        private final String lastError;

        @JsonCreator
        StatusData(@JsonProperty("currentPartitions") long currentPartitions,
                   @JsonProperty("lastSuccessful") boolean lastAttemptSuccessful,
                   @JsonProperty("lastError") String lastError) {
            this.currentPartitions = currentPartitions;
            this.lastAttemptSuccessful = lastAttemptSuccessful;
            this.lastError = lastError;
        }

        @JsonProperty
        public long currentPartitions() {
            return currentPartitions;
        }

        @JsonProperty
        public boolean lastAttemptSuccessful() {
            return lastAttemptSuccessful;
        }

        @JsonProperty
        public String lastError() {
            return lastError;
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("PartitionGrowerWorker is not running.");
        }
        log.info("{}: Deactivating PartitionGrowerWorker.", id);
        this.statusUpdaterFuture.cancel(false);
        doneFuture.complete("");
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.DAYS);
        this.executor = null;
        new StatusUpdater().run();

        this.status = null;
    }
}
