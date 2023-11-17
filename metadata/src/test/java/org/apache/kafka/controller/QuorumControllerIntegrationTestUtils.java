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

package org.apache.kafka.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.Listener;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.server.common.MetadataVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Utility functions for use in QuorumController integration tests.
 */
public class QuorumControllerIntegrationTestUtils {
    private final static Logger log = LoggerFactory.getLogger(QuorumControllerIntegrationTestUtils.class);

    BrokerRegistrationRequestData.FeatureCollection brokerFeatures() {
        return brokerFeatures(MetadataVersion.MINIMUM_KRAFT_VERSION, MetadataVersion.latest());
    }

    /**
     * Create a broker features collection for use in a registration request. We only set MV. here.
     *
     * @param minVersion    The minimum supported MV.
     * @param maxVersion    The maximum supported MV.
     */
    static BrokerRegistrationRequestData.FeatureCollection brokerFeatures(
        MetadataVersion minVersion,
        MetadataVersion maxVersion
    ) {
        BrokerRegistrationRequestData.FeatureCollection features = new BrokerRegistrationRequestData.FeatureCollection();
        features.add(new BrokerRegistrationRequestData.Feature()
                         .setName(MetadataVersion.FEATURE_NAME)
                         .setMinSupportedVersion(minVersion.featureLevel())
                         .setMaxSupportedVersion(maxVersion.featureLevel()));
        return features;
    }

    /**
     * Register the given number of brokers.
     *
     * @param controller    The active controller.
     * @param numBrokers    The number of brokers to register. We will start at 0 and increment.
     *
     * @return              A map from broker IDs to broker epochs.
     */
    static Map<Integer, Long> registerBrokersAndUnfence(
        QuorumController controller,
        int numBrokers
    ) throws Exception {
        Map<Integer, Long> brokerEpochs = new HashMap<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            BrokerRegistrationReply reply = controller.registerBroker(ANONYMOUS_CONTEXT,
                new BrokerRegistrationRequestData()
                    .setBrokerId(brokerId)
                    .setRack(null)
                    .setClusterId(controller.clusterId())
                    .setFeatures(brokerFeatures(MetadataVersion.IBP_3_0_IV1, MetadataVersion.latest()))
                    .setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + brokerId))
                    .setListeners(new ListenerCollection(
                        Arrays.asList(
                            new Listener()
                                .setName("PLAINTEXT")
                                .setHost("localhost")
                                .setPort(9092 + brokerId)
                            ).iterator()
                        )
                    )
            ).get();
            brokerEpochs.put(brokerId, reply.epoch());

            // Send heartbeat to unfence
            controller.processBrokerHeartbeat(ANONYMOUS_CONTEXT,
                new BrokerHeartbeatRequestData()
                    .setWantFence(false)
                    .setBrokerEpoch(brokerEpochs.get(brokerId))
                    .setBrokerId(brokerId)
                    .setCurrentMetadataOffset(100000L)
            ).get();
        }

        return brokerEpochs;
    }

    /**
     * Send broker heartbeats for the provided brokers.
     *
     * @param controller    The active controller.
     * @param brokers       The broker IDs to send heartbeats for.
     * @param brokerEpochs  A map from broker ID to broker epoch.
     */
    static void sendBrokerHeartbeat(
        QuorumController controller,
        List<Integer> brokers,
        Map<Integer, Long> brokerEpochs
    ) throws Exception {
        if (brokers.isEmpty()) {
            return;
        }
        for (Integer brokerId : brokers) {
            BrokerHeartbeatReply reply = controller.processBrokerHeartbeat(ANONYMOUS_CONTEXT,
                new BrokerHeartbeatRequestData()
                    .setWantFence(false)
                    .setBrokerEpoch(brokerEpochs.get(brokerId))
                    .setBrokerId(brokerId)
                    .setCurrentMetadataOffset(100000)
            ).get();
            assertEquals(new BrokerHeartbeatReply(true, false, false, false), reply);
        }
    }

    /**
     * Create some topics directly on the controller.
     *
     * @param controller            The active controller.
     * @param prefix                The prefix to use for topic names.
     * @param numTopics             The number of topics to create.
     * @param replicationFactor     The replication factor to use.
     */
    static void createTopics(
        QuorumController controller,
        String prefix,
        int numTopics,
        int replicationFactor
    ) throws Exception {
        HashSet<String> describable = new HashSet<>();
        for (int i = 0; i < numTopics; i++) {
            describable.add(prefix + i);
        }
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        for (int i = 0; i < numTopics; i++) {
            request.topics().add(
                new CreatableTopic().
                    setName(prefix + i).
                    setNumPartitions(1).
                    setReplicationFactor((short) replicationFactor));
        }
        CreateTopicsResponseData response =
            controller.createTopics(ANONYMOUS_CONTEXT, request, describable).get();
        for (int i = 0; i < numTopics; i++) {
            CreatableTopicResult result = response.topics().find(prefix + i);
            if (result.errorCode() != Errors.TOPIC_ALREADY_EXISTS.code()) {
                assertEquals((short) 0, result.errorCode());
            }
        }
    }

    /**
     * Add an event to the controller event queue that will pause it temporarily.
     *
     * @param controller    The controller.
     * @return              The latch that can be used to unpause the controller.
     */
    public static CountDownLatch pause(QuorumController controller) {
        final CountDownLatch latch = new CountDownLatch(1);
        controller.appendControlEvent("pause", () -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.info("Interrupted while waiting for unpause.", e);
            }
        });
        return latch;
    }

    /**
     * Force the current controller to renounce.
     *
     * @param controller    The controller.
     */
    static void forceRenounce(QuorumController controller) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        controller.appendControlEvent("forceRenounce", () -> {
            controller.renounce();
            future.complete(null);
        });
        future.get(30, TimeUnit.SECONDS);
    }
}
