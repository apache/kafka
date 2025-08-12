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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.MetadataVersion;

import org.slf4j.Logger;

/**
 * Tracks the registration of a specific broker, and executes a callback if it should be refreshed.
 *
 * This tracker handles cases where we might want to re-register the broker. The only such case
 * right now is during the transition from non-JBOD mode, to JBOD mode. In other words, the
 * transition from a MetadataVersion less than 3.7-IV2, to one greater than or equal to 3.7-IV2.
 * In this case, the broker registration will start out containing no directories, and we need to
 * resend the BrokerRegistrationRequest to fix that.
 *
 * As much as possible, the goal here is to keep things simple. We just compare the desired state
 * with the actual state, and try to make changes only if necessary.
 */
public class BrokerRegistrationTracker implements MetadataPublisher {
    private final Logger log;
    private final int id;
    private final Runnable refreshRegistrationCallback;

    /**
     * Create the tracker.
     *
     * @param id                            The ID of this broker.
     * @param refreshRegistrationCallback   Callback to run if we need to refresh the registration.
     */
    public BrokerRegistrationTracker(
        int id,
        Runnable refreshRegistrationCallback
    ) {
        this.log = new LogContext("[BrokerRegistrationTracker id=" + id + "] ").
            logger(BrokerRegistrationTracker.class);
        this.id = id;
        this.refreshRegistrationCallback = refreshRegistrationCallback;
    }

    @Override
    public String name() {
        return "BrokerRegistrationTracker(id=" + id + ")";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        boolean checkBrokerRegistration = false;
        if (delta.featuresDelta() != null) {
            if (delta.metadataVersionChanged().isPresent()) {
                if (log.isTraceEnabled()) {
                    log.trace("Metadata version change is present: {}",
                        delta.metadataVersionChanged());
                }
                checkBrokerRegistration = true;
            }
        }
        if (delta.clusterDelta() != null) {
            if (delta.clusterDelta().changedBrokers().get(id) != null) {
                if (log.isTraceEnabled()) {
                    log.trace("Broker change is present: {}",
                        delta.clusterDelta().changedBrokers().get(id));
                }
                checkBrokerRegistration = true;
            }
        }
        if (checkBrokerRegistration) {
            if (brokerRegistrationNeedsRefresh(newImage.features().metadataVersionOrThrow(), delta.clusterDelta().broker(id))) {
                refreshRegistrationCallback.run();
            }
        }
    }

    /**
     * Check if the current broker registration needs to be refreshed.
     *
     * @param metadataVersion   The current metadata version.
     * @param registration      The current broker registration, or null if there is none.
     * @return                  True only if we should refresh.
     */
    boolean brokerRegistrationNeedsRefresh(
        MetadataVersion metadataVersion,
        BrokerRegistration registration
    ) {
        // If there is no existing registration, the BrokerLifecycleManager must still be sending it.
        // So we don't need to do anything yet.
        if (registration == null) {
            log.debug("No current broker registration to check.");
            return false;
        }
        // Check to see if the directory list has changed.  Note that this check could certainly be
        // triggered spuriously. For example, if the broker's directory list has been changed in the
        // past, and we are in the process of replaying that change log, we will end up here.
        // That's fine because resending the broker registration does not cause any problems. And,
        // of course, as soon as a snapshot is made, we will no longer need to worry about those
        // old metadata log entries being replayed on startup.
        if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_7_IV2) &&
                registration.directories().isEmpty()) {
            log.info("Current directory set is empty, but MV supports JBOD. Resending " +
                    "broker registration.");
            return true;
        }
        log.debug("Broker registration does not need to be resent.");
        return false;
    }
}
