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

package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Configurable;

import java.util.Map;
import java.security.Principal;

/*
 * PrincipalBuilder for Authenticator
 */
@InterfaceStability.Unstable
public interface PrincipalBuilder extends Configurable {

    /**
     * Configures this class with given key-value pairs.
     */
    void configure(Map<String, ?> configs);

    /**
     * Returns Principal.
     */
    Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException;

    /**
     * Closes this instance.
     */
    void close() throws KafkaException;

}
