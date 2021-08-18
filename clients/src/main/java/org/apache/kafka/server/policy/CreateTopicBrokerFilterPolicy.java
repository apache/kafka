/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;

/**
 * <p>Determines whether partitions should be placed on brokers as new topics or assignments are created.
 */
public interface CreateTopicBrokerFilterPolicy extends Configurable, AutoCloseable {
    class Broker {
        private final int brokerId;
        private final String rack;

        public Broker(final int brokerId, final String rack) {
            this.brokerId = brokerId;
            this.rack = rack;
        }

        public int getBrokerId() {
            return brokerId;
        }

        public String getRack() {
            return rack;
        }

        @Override
        public String toString() {
            return "Broker{" +
                "brokerId=" + brokerId +
                ", rack='" + rack + '\'' +
                '}';
        }
    }

    /**
     * Determines whether partitions should be placed on a broker.
     * @param broker  a broker in the cluster
     * @return        <code>true</code> if the broker is allowed to host partitions
     */
    boolean isAllowedToHostPartitions(Broker broker);
}
