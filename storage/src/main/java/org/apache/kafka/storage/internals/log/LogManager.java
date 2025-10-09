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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.PartitionRegistration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class LogManager {

    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    public static final String LOCK_FILE_NAME = ".lock";
    public static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";
    public static final String LOG_START_OFFSET_CHECKPOINT_FILE = "log-start-offset-checkpoint";

    /**
     * Wait for all jobs to complete
     * @param jobs The jobs
     * @param callback This will be called to handle the exception caused by each Future#get
     * @return true if all pass. Otherwise, false
     */
    public static boolean waitForAllToComplete(List<Future<?>> jobs, Consumer<Throwable> callback) {
        List<Future<?>> failed = new ArrayList<>();
        for (Future<?> job : jobs) {
            try {
                job.get();
            } catch (Exception e) {
                callback.accept(e);
                failed.add(job);
            }
        }
        return failed.isEmpty();
    }

    /**
     * Returns true if the given log should not be on the current broker
     * according to the metadata image.
     *
     * @param brokerId       The ID of the current broker.
     * @param newTopicsImage The new topics image after broker has been reloaded
     * @param log            The log object to check
     * @return true if the log should not exist on the broker, false otherwise.
     */
    public static boolean isStrayKraftReplica(int brokerId, TopicsImage newTopicsImage, UnifiedLog log) {
        if (log.topicId().isEmpty()) {
            // Missing topic ID could result from storage failure or unclean shutdown after topic creation but before flushing
            // data to the `partition.metadata` file. And before appending data to the log, the `partition.metadata` is always
            // flushed to disk. So if the topic ID is missing, it mostly means no data was appended, and we can treat this as
            // a stray log.
            LOG.info("The topicId does not exist in {}, treat it as a stray log.", log);
            return true;
        }

        Uuid topicId = log.topicId().get();
        int partitionId = log.topicPartition().partition();
        PartitionRegistration partition = newTopicsImage.getPartition(topicId, partitionId);
        if (partition == null) {
            LOG.info("Found stray log dir {}: the topicId {} does not exist in the metadata image.", log, topicId);
            return true;
        } else {
            List<Integer> replicas = Arrays.stream(partition.replicas).boxed().toList();
            if (!replicas.contains(brokerId)) {
                LOG.info("Found stray log dir {}: the current replica assignment {} does not contain the local brokerId {}.",
                        log, replicas.stream().map(String::valueOf).collect(Collectors.joining(", ", "[", "]")), brokerId);
                return true;
            } else {
                return false;
            }
        }
    }
}
