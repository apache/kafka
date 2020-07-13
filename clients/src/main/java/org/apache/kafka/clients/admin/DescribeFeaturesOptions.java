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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

@InterfaceStability.Evolving
public class DescribeFeaturesOptions extends AbstractOptions<DescribeFeaturesOptions> {
    /**
     * - True means the {@link Admin#describeFeatures(DescribeFeaturesOptions)} request can be
     *   issued only to the controller.
     * - False means the {@link Admin#describeFeatures(DescribeFeaturesOptions)} request can be
     *   issued to any random broker.
     */
    private boolean shouldSendRequestToController = false;

    /**
     * Sets a flag indicating that the describe features request should be issued to the controller.
     */
    public DescribeFeaturesOptions sendRequestToController(boolean shouldSendRequestToController) {
        this.shouldSendRequestToController = shouldSendRequestToController;
        return this;
    }

    public boolean sendRequestToController() {
        return shouldSendRequestToController;
    }

    /**
     * Sets the timeout in milliseconds for this operation or {@code null} if the default API
     * timeout for the AdminClient should be used.
     */
    public DescribeFeaturesOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }
}
