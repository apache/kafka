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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.metadata.BrokerRegistration;

@InterfaceStability.Stable
public enum BrokerRegistrationState {
    UNREGISTERED(-1),
    FENCED(10),
    CONTROLLED_SHUTDOWN(20),
    ACTIVE(30);

    private final int state;

    BrokerRegistrationState(int state) {
        this.state = state;
    }

    public int state() {
        return state;
    }

    public static BrokerRegistrationState getBrokerRegistrationState(BrokerRegistration brokerRegistration) {
        if (brokerRegistration.fenced()) {
            return BrokerRegistrationState.FENCED;
        } else if (brokerRegistration.inControlledShutdown()) {
            return BrokerRegistrationState.CONTROLLED_SHUTDOWN;
        } else {
            return BrokerRegistrationState.ACTIVE;
        }
    }
}
