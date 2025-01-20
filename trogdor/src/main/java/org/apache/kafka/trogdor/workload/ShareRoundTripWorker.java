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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.trogdor.common.WorkerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ShareRoundTripWorker extends RoundTripWorkerBase {
    private static final Logger log = LoggerFactory.getLogger(ShareRoundTripWorker.class);
    KafkaShareConsumer<byte[], byte[]> consumer;
    ShareRoundTripWorker(String id, RoundTripWorkloadSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void initializeConsumer(HashSet<TopicPartition> partitions) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer." + id);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000);
        // user may over-write the defaults with common client config and consumer config
        WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf());

        String groupId = "round-trip-share-group-" + id;
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        try (Admin adminClient = WorkerUtils.createAdminClient(spec.bootstrapServers(), spec.commonClientConf(), spec.adminClientConf())) {
            alterShareAutoOffsetReset(groupId, "earliest", adminClient);
        } catch (Exception e) {
            log.warn("Failed to set share.auto.offset.reset config to 'earliest' mode", e);
            throw e;
        }

        consumer = new KafkaShareConsumer<>(props, new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
        consumer.subscribe(spec.activeTopics().materialize().keySet());
    }

    @Override
    protected ConsumerRecords<byte[], byte[]> fetchRecords(Duration duration) {
        return consumer.poll(duration);
    }

    @Override
    protected void shutdownConsumer() {
        Utils.closeQuietly(consumer, "consumer");
        consumer = null;
    }

    private void alterShareAutoOffsetReset(String groupId, String newValue, Admin adminClient) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
        alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, newValue), AlterConfigOp.OpType.SET)));
        AlterConfigsOptions alterOptions = new AlterConfigsOptions();
        try {
            adminClient.incrementalAlterConfigs(alterEntries, alterOptions)
                    .all()
                    .get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Exception was thrown while attempting to set share.auto.offset.reset config: ", e);
        }
    }
}
