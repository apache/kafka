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
package org.apache.kafka.common.security.scram.internals;

import java.security.Provider;
import java.security.Security;

import org.apache.kafka.common.security.scram.internals.ScramSaslClient.ScramSaslClientFactory;

public class ScramSaslClientProvider extends Provider {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings({"deprecation", "this-escape"})
    protected ScramSaslClientProvider() {
        super("SASL/SCRAM Client Provider", 1.0, "SASL/SCRAM Client Provider for Kafka");
        for (ScramMechanism mechanism : ScramMechanism.values())
            put("SaslClientFactory." + mechanism.mechanismName(), ScramSaslClientFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new ScramSaslClientProvider());
    }
}
