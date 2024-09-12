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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

/**
 * This class just provides a static name for the methods in the {@link ConsumerRebalanceListener} interface
 * for a bit more compile time assurance.
 */
public enum ConsumerRebalanceListenerMethodName {

    ON_PARTITIONS_REVOKED("onPartitionsRevoked"),
    ON_PARTITIONS_ASSIGNED("onPartitionsAssigned"),
    ON_PARTITIONS_LOST("onPartitionsLost");

    private final String fullyQualifiedMethodName;

    ConsumerRebalanceListenerMethodName(String methodName) {
        this.fullyQualifiedMethodName = String.format("%s.%s", ConsumerRebalanceListener.class.getSimpleName(), methodName);
    }

    /**
     * Provides the fully-qualified method name, e.g. {@code ConsumerRebalanceListener.onPartitionsRevoked}. This
     * is used for log messages.
     *
     * @return Fully-qualified method name
     */
    public String fullyQualifiedMethodName() {
        return fullyQualifiedMethodName;
    }
}
