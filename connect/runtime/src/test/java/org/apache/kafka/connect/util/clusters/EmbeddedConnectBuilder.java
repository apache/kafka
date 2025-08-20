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
package org.apache.kafka.connect.util.clusters;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

abstract class EmbeddedConnectBuilder<C extends EmbeddedConnect, B extends EmbeddedConnectBuilder<C, B>> {
    private Map<String, String> workerProps = new HashMap<>();
    private int numBrokers = EmbeddedConnect.DEFAULT_NUM_BROKERS;
    private Properties brokerProps = new Properties();
    private boolean maskExitProcedures = true;
    private final Map<String, String> clientProps = new HashMap<>();

    protected abstract C build(
            int numBrokers,
            Properties brokerProps,
            boolean maskExitProcedures,
            Map<String, String> clientProps,
            Map<String, String> workerProps
    );

    public B workerProps(Map<String, String> workerProps) {
        this.workerProps = workerProps;
        return self();
    }

    public B numBrokers(int numBrokers) {
        this.numBrokers = numBrokers;
        return self();
    }

    public B brokerProps(Properties brokerProps) {
        this.brokerProps = brokerProps;
        return self();
    }

    public B clientProps(Map<String, String> clientProps) {
        this.clientProps.putAll(clientProps);
        return self();
    }

    /**
     * In the event of ungraceful shutdown, embedded clusters call exit or halt with non-zero
     * exit statuses. Exiting with a non-zero status forces a test to fail and is hard to
     * handle. Because graceful exit is usually not required during a test and because
     * depending on such an exit increases flakiness, this setting allows masking
     * exit and halt procedures by using a runtime exception instead. Customization of the
     * exit and halt procedures is possible through {@code exitProcedure} and {@code
     * haltProcedure} respectively.
     *
     * @param mask if false, exit and halt procedures remain unchanged; true is the default.
     * @return the builder for this cluster
     */
    public B maskExitProcedures(boolean mask) {
        this.maskExitProcedures = mask;
        return self();
    }

    public C build() {
        return build(numBrokers, brokerProps, maskExitProcedures, clientProps, workerProps);
    }

    @SuppressWarnings("unchecked")
    protected B self() {
        return (B) this;
    }

}
